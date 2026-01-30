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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.TreeMultimap;
import com.google.crypto.tink.Aead;
import io.confluent.dekregistry.client.DekRegistryClient;
import io.confluent.dekregistry.client.rest.DekRegistryRestService;
import io.confluent.dekregistry.client.rest.entities.CreateDekRequest;
import io.confluent.dekregistry.client.rest.entities.CreateKekRequest;
import io.confluent.dekregistry.client.rest.entities.Dek;
import io.confluent.dekregistry.client.rest.entities.Kek;
import io.confluent.dekregistry.client.rest.entities.KeyType;
import io.confluent.dekregistry.client.rest.entities.UpdateKekRequest;
import io.confluent.dekregistry.metrics.MetricsManager;
import io.confluent.dekregistry.storage.exceptions.AlreadyExistsException;
import io.confluent.dekregistry.storage.exceptions.DekGenerationException;
import io.confluent.dekregistry.storage.exceptions.InvalidKeyException;
import io.confluent.dekregistry.storage.exceptions.KeyNotSoftDeletedException;
import io.confluent.dekregistry.storage.exceptions.KeyReferenceExistsException;
import io.confluent.dekregistry.storage.exceptions.KeySoftDeletedException;
import io.confluent.dekregistry.storage.exceptions.TooManyKeysException;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.rest.utils.UrlList;
import io.confluent.kafka.schemaregistry.encryption.tink.Cryptor;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryRequestForwardingException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.exceptions.UnknownLeaderException;
import io.confluent.kafka.schemaregistry.storage.Mode;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import io.confluent.rest.exceptions.RestException;
import io.kcache.Cache;
import io.kcache.KeyValue;
import jakarta.ws.rs.core.UriBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for DEK Registry implementations.
 * Contains encryption/decryption logic and public API methods.
 * Subclasses implement the storage-specific methods.
 */
public abstract class AbstractDekRegistry implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(AbstractDekRegistry.class);

  public static final String KEY = "dekRegistry";

  public static final int LATEST_VERSION = DekRegistryClient.LATEST_VERSION;
  public static final int MIN_VERSION = 1;
  public static final byte[] EMPTY_AAD = new byte[0];
  public static final String X_FORWARD_HEADER = DekRegistryRestService.X_FORWARD_HEADER;

  public static final String AWS_KMS = "aws-kms";
  public static final String AZURE_KMS = "azure-kms";
  public static final String GCP_KMS = "gcp-kms";

  protected static final String TEST_SUBJECT = "__TEST";

  protected static final TypeReference<Kek> KEK_TYPE =
      new TypeReference<Kek>() {
      };
  protected static final TypeReference<Dek> DEK_TYPE =
      new TypeReference<Dek>() {
      };
  protected static final TypeReference<Void> VOID_TYPE =
      new TypeReference<Void>() {
      };

  private final SchemaRegistry schemaRegistry;
  private final MetricsManager metricsManager;
  private final DekRegistryConfig config;
  private final SetMultimap<String, KeyEncryptionKeyId> sharedKeys;

  protected final int kekSearchDefaultLimit;
  protected final int kekSearchMaxLimit;
  protected final int dekSubjectSearchDefaultLimit;
  protected final int dekSubjectSearchMaxLimit;
  protected final int dekVersionSearchDefaultLimit;
  protected final int dekVersionSearchMaxLimit;

  protected final Map<DekFormat, Cryptor> cryptors;
  protected final Map<String, Lock> tenantToLock = new ConcurrentHashMap<>();
  protected final AtomicBoolean initialized = new AtomicBoolean();
  protected final CountDownLatch initLatch = new CountDownLatch(1);

  protected AbstractDekRegistry(
      SchemaRegistry schemaRegistry,
      MetricsManager metricsManager,
      DekRegistryConfig config
  ) {
    this.schemaRegistry = schemaRegistry;
    this.schemaRegistry.properties().put(KEY, this);
    this.metricsManager = metricsManager;
    this.config = config;
    this.sharedKeys = Multimaps.synchronizedSetMultimap(TreeMultimap.create());
    this.cryptors = new ConcurrentHashMap<>();
    this.kekSearchDefaultLimit =
        config.getInt(DekRegistryConfig.KEK_SEARCH_DEFAULT_LIMIT_CONFIG);
    this.kekSearchMaxLimit = config.getInt(DekRegistryConfig.KEK_SEARCH_MAX_LIMIT_CONFIG);
    this.dekSubjectSearchDefaultLimit =
        config.getInt(DekRegistryConfig.DEK_SUBJECT_SEARCH_DEFAULT_LIMIT_CONFIG);
    this.dekSubjectSearchMaxLimit =
        config.getInt(DekRegistryConfig.DEK_SUBJECT_SEARCH_MAX_LIMIT_CONFIG);
    this.dekVersionSearchDefaultLimit =
        config.getInt(DekRegistryConfig.DEK_VERSION_SEARCH_DEFAULT_LIMIT_CONFIG);
    this.dekVersionSearchMaxLimit =
        config.getInt(DekRegistryConfig.DEK_VERSION_SEARCH_MAX_LIMIT_CONFIG);
  }

  // ==================== Abstract Storage Methods ====================

  /**
   * Get all KEKs for a tenant.
   */
  protected abstract List<KeyValue<EncryptionKeyId, EncryptionKey>> getKeksFromStore(
      String tenant, boolean lookupDeleted) throws SchemaRegistryStoreException;

  /**
   * Get all DEKs for a tenant within a KEK name range.
   */
  protected abstract List<KeyValue<EncryptionKeyId, EncryptionKey>> getDeksFromStore(
      String tenant, String minKekName, String maxKekName, boolean lookupDeleted)
      throws SchemaRegistryStoreException;

  /**
   * Get DEKs for a specific subject and algorithm.
   */
  protected abstract List<KeyValue<EncryptionKeyId, EncryptionKey>> getDeksFromStore(
      String tenant, String kekName, String subject, DekFormat algorithm, boolean lookupDeleted)
      throws SchemaRegistryStoreException;

  /**
   * Get a KEK by its ID.
   */
  protected abstract KeyEncryptionKey getKekById(KeyEncryptionKeyId keyId) throws
      SchemaRegistryStoreException;

  /**
   * Get a DEK by its ID.
   */
  protected abstract DataEncryptionKey getDekById(DataEncryptionKeyId keyId) throws
      SchemaRegistryStoreException;

  /**
   * Store a key (KEK or DEK).
   */
  protected abstract void putKey(EncryptionKeyId id, EncryptionKey key) throws
      SchemaRegistryStoreException;

  /**
   * Remove a key from storage.
   */
  protected abstract void removeKey(EncryptionKeyId id) throws SchemaRegistryStoreException;

  /**
   * Sync/refresh the store to ensure it's up-to-date.
   */
  protected abstract void syncStore() throws SchemaRegistryStoreException;

  /**
   * Perform any initialization required by the storage backend.
   */
  protected abstract void initStore();

  // ==================== Accessors ====================

  public SchemaRegistry getSchemaRegistry() {
    return schemaRegistry;
  }

  public MetricsManager getMetricsManager() {
    return metricsManager;
  }

  public DekRegistryConfig config() {
    return config;
  }

  public SetMultimap<String, KeyEncryptionKeyId> getSharedKeys() {
    return sharedKeys;
  }

  /**
   * Get the underlying keys kcache (only kafka-based implementations override this).
   * Provides backward compatibility for external components relying on direct cache access.
   * @return the keys cache
   */
  @Deprecated
  public Cache<EncryptionKeyId, EncryptionKey> keys() {
    throw new UnsupportedOperationException(
        "Direct access to the keys cache is not supported in AbstractDekRegistry");
  }

  // ==================== Cryptor Management ====================

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

  // ==================== Initialization ====================

  public void init() {
    if (!initialized.get()) {
      initStore();
      boolean isInitialized = initialized.compareAndSet(false, true);
      if (!isInitialized) {
        throw new IllegalStateException("AbstractDekRegistry was already initialized");
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

  // ==================== Leader/Locking ====================

  /**
   * Check if this instance is the leader.
   */
  public boolean isLeader() {
    return schemaRegistry.isLeader();
  }

  protected boolean isLeader(Map<String, String> headerProperties) {
    String forwardHeader = headerProperties.get(X_FORWARD_HEADER);
    return isLeader() && (forwardHeader == null || Boolean.parseBoolean(forwardHeader));
  }

  protected Lock lockFor(String tenant) {
    return tenantToLock.computeIfAbsent(tenant, k -> new ReentrantLock());
  }

  protected void lock(String tenant, Map<String, String> headerProperties) {
    String forwardHeader = headerProperties.get(X_FORWARD_HEADER);
    if (forwardHeader == null || Boolean.parseBoolean(forwardHeader)) {
      lockFor(tenant).lock();
    }
  }

  protected void unlock(String tenant) {
    if (((ReentrantLock) lockFor(tenant)).isHeldByCurrentThread()) {
      lockFor(tenant).unlock();
    }
  }

  // ==================== KEK Operations ====================

  public List<String> getKekNames(List<String> subjectPrefix, boolean lookupDeleted) {
    String tenant = schemaRegistry.tenant();
    try {
      if (subjectPrefix == null || subjectPrefix.isEmpty()) {
        return getKeks(tenant, lookupDeleted).stream()
            .map(kv -> ((KeyEncryptionKeyId) kv.key).getName())
            .collect(Collectors.toList());
      } else {
        return getDeks(tenant, lookupDeleted).stream()
            .filter(kv -> subjectPrefix.stream()
                .anyMatch(prefix -> ((DataEncryptionKeyId) kv.key).getSubject().startsWith(prefix)))
            .map(kv -> ((DataEncryptionKeyId) kv.key).getKekName())
            .sorted()
            .distinct()
            .collect(Collectors.toList());
      }
    } catch (SchemaRegistryStoreException e) {
      throw new RuntimeException("Error retrieving KEK names", e);
    }
  }

  protected List<KeyValue<EncryptionKeyId, EncryptionKey>> getKeks(
      String tenant, boolean lookupDeleted) throws SchemaRegistryStoreException {
    return getKeksFromStore(tenant, lookupDeleted);
  }

  public KeyEncryptionKey getKek(String name, boolean lookupDeleted) {
    String tenant = schemaRegistry.tenant();
    KeyEncryptionKeyId keyId = new KeyEncryptionKeyId(tenant, name);
    try {
      KeyEncryptionKey key = getKekById(keyId);
      if (key != null && (!key.isDeleted() || lookupDeleted)) {
        return key;
      } else {
        return null;
      }
    } catch (SchemaRegistryStoreException e) {
      throw new RuntimeException("Error retrieving KEK", e);
    }
  }

  public Kek toKekEntity(KeyEncryptionKey kek) {
    return kek.toKekEntity();
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
    // Ensure store is up-to-date
    syncStore();

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
    KeyEncryptionKey oldKey = getKekById(keyId);
    // Allow create to act like undelete if the kek is deleted
    if (oldKey != null
        && (request.isDeleted() == oldKey.isDeleted() || !oldKey.isEquivalent(key))) {
      throw new AlreadyExistsException(request.getName());
    }
    putKey(keyId, key);
    // Retrieve key with ts set
    key = getKekById(keyId);
    return key;
  }

  protected String normalizeKmsType(String kmsType) {
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

  public void testKek(KeyEncryptionKey kek) throws SchemaRegistryException {
    DataEncryptionKey key = new DataEncryptionKey(kek.getName(), TEST_SUBJECT,
        DekFormat.AES256_GCM, MIN_VERSION, null, false);
    if (kek.isShared()) {
      generateEncryptedDek(kek, key);
    } else {
      throw new InvalidKeyException("shared");
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
    // Ensure store is up-to-date
    syncStore();

    String tenant = schemaRegistry.tenant();
    KeyEncryptionKeyId keyId = new KeyEncryptionKeyId(tenant, name);
    KeyEncryptionKey key = getKekById(keyId);
    if (key == null || key.isDeleted()) {
      return null;
    }
    SortedMap<String, String> kmsProps = request.getOptionalKmsProps() != null
        ? (request.getOptionalKmsProps().isPresent() ? new TreeMap<>(request.getKmsProps()) : null)
        : key.getKmsProps();
    String doc = request.getOptionalDoc().orElse(key.getDoc());
    boolean shared = request.isOptionalShared().orElse(key.isShared());
    KeyEncryptionKey newKey = new KeyEncryptionKey(name, key.getKmsType(),
        key.getKmsKeyId(), kmsProps, doc, shared, false);
    if (newKey.isEquivalent(key)) {
      return key;
    }
    putKey(keyId, newKey);
    // Retrieve key with ts set
    newKey = getKekById(keyId);
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
    // Ensure store is up-to-date
    syncStore();

    String tenant = schemaRegistry.tenant();
    if (!getDeks(tenant, name, permanentDelete).isEmpty()) {
      throw new KeyReferenceExistsException(name);
    }
    KeyEncryptionKeyId keyId = new KeyEncryptionKeyId(tenant, name);
    KeyEncryptionKey oldKey = getKekById(keyId);
    if (oldKey == null) {
      return;
    }
    if (permanentDelete) {
      if (!oldKey.isDeleted()) {
        throw new KeyNotSoftDeletedException(name);
      }
      removeKey(keyId);
    } else {
      if (!oldKey.isDeleted()) {
        KeyEncryptionKey newKey = new KeyEncryptionKey(name, oldKey.getKmsType(),
            oldKey.getKmsKeyId(), oldKey.getKmsProps(), oldKey.getDoc(), oldKey.isShared(), true);
        putKey(keyId, newKey);
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
    // Ensure store is up-to-date
    syncStore();

    String tenant = schemaRegistry.tenant();
    KeyEncryptionKeyId keyId = new KeyEncryptionKeyId(tenant, name);
    KeyEncryptionKey key = getKekById(keyId);
    if (key == null) {
      return;
    }
    if (key.isDeleted()) {
      KeyEncryptionKey newKey = new KeyEncryptionKey(name, key.getKmsType(),
          key.getKmsKeyId(), key.getKmsProps(), key.getDoc(), key.isShared(), false);
      putKey(keyId, newKey);
    }
  }

  // ==================== DEK Operations ====================

  public List<String> getDekSubjects(String kekName, boolean lookupDeleted) {
    String tenant = schemaRegistry.tenant();
    try {
      return getDeks(tenant, kekName, lookupDeleted).stream()
          .map(kv -> ((DataEncryptionKeyId) kv.key).getSubject())
          .sorted()
          .distinct()
          .collect(Collectors.toList());
    } catch (SchemaRegistryStoreException e) {
      throw new RuntimeException("Error retrieving DEK subjects", e);
    }
  }

  public List<Integer> getDekVersions(
      String kekName, String subject, DekFormat algorithm, boolean lookupDeleted) {
    String tenant = schemaRegistry.tenant();
    try {
      return getDeks(tenant, kekName, subject, algorithm, lookupDeleted).stream()
          .map(kv -> ((DataEncryptionKeyId) kv.key).getVersion())
          .collect(Collectors.toList());
    } catch (SchemaRegistryStoreException e) {
      throw new RuntimeException("Error retrieving DEK versions", e);
    }
  }

  protected List<KeyValue<EncryptionKeyId, EncryptionKey>> getDeks(
      String tenant, boolean lookupDeleted) throws SchemaRegistryStoreException {
    return getDeksFromStore(tenant, String.valueOf(Character.MIN_VALUE),
        String.valueOf(Character.MAX_VALUE), lookupDeleted);
  }

  protected List<KeyValue<EncryptionKeyId, EncryptionKey>> getDeks(
      String tenant, String kekName, boolean lookupDeleted) throws SchemaRegistryStoreException {
    return getDeksFromStore(tenant, kekName, kekName, lookupDeleted);
  }

  protected List<KeyValue<EncryptionKeyId, EncryptionKey>> getDeks(
      String tenant, String kekName, String subject, DekFormat algorithm, boolean lookupDeleted)
      throws SchemaRegistryStoreException {
    return getDeksFromStore(tenant, kekName, subject, algorithm, lookupDeleted);
  }

  public DataEncryptionKey getLatestDek(String kekName, String subject, DekFormat algorithm,
      boolean lookupDeleted) throws SchemaRegistryException {
    return getLatestDek(kekName, subject, algorithm, lookupDeleted, true);
  }

  public DataEncryptionKey getLatestDek(String kekName, String subject, DekFormat algorithm,
      boolean lookupDeleted, boolean maybeGenerateRawDek) throws SchemaRegistryException {
    String tenant = schemaRegistry.tenant();
    List<KeyValue<EncryptionKeyId, EncryptionKey>> deks =
        getDeks(tenant, kekName, subject, algorithm, lookupDeleted);
    Collections.reverse(deks);
    if (deks.isEmpty()) {
      return null;
    }
    DataEncryptionKey dek = (DataEncryptionKey) deks.get(0).value;
    return maybeGenerateRawDek ? maybeGenerateRawDek(dek) : dek;
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
    DataEncryptionKey key = getDekById(keyId);
    if (key != null && (!key.isDeleted() || lookupDeleted)) {
      return maybeGenerateRawDek(key);
    } else {
      return null;
    }
  }

  protected DataEncryptionKey maybeGenerateRawDek(DataEncryptionKey key)
      throws SchemaRegistryException {
    KeyEncryptionKey kek = getKek(key.getKekName(), true);
    if (kek.isShared()) {
      key = generateRawDek(kek, key);
    }
    return key;
  }

  public Dek createDekOrForward(String kekName, boolean rewrap, CreateDekRequest request,
      Map<String, String> headerProperties) throws SchemaRegistryException {
    String tenant = schemaRegistry.tenant();
    lock(tenant, headerProperties);
    try {
      if (isLeader(headerProperties)) {
        return createDek(kekName, rewrap, request).toDekEntity();
      } else {
        // forward registering request to the leader
        if (schemaRegistry.leaderIdentity() != null) {
          return forwardCreateDekRequestToLeader(kekName, rewrap, request, headerProperties);
        } else {
          throw new UnknownLeaderException("Request failed since leader is unknown");
        }
      }
    } finally {
      unlock(tenant);
    }
  }

  private Dek forwardCreateDekRequestToLeader(String kekName, boolean rewrap,
      CreateDekRequest request, Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    RestService leaderRestService = schemaRegistry.leaderRestService();
    final UrlList baseUrl = leaderRestService.getBaseUrls();

    UriBuilder builder = UriBuilder.fromPath("/dek-registry/v1/keks/{name}/deks/{subject}");
    String path = builder
        .queryParam("rewrap", rewrap)
        .build(kekName, request.getSubject()).toString();

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

  public DataEncryptionKey createDek(String kekName, boolean rewrap, CreateDekRequest request)
      throws SchemaRegistryException {
    // Ensure store is up-to-date
    syncStore();

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
    DataEncryptionKeyId keyId = new DataEncryptionKeyId(
        tenant, kekName, request.getSubject(), algorithm, version);
    DataEncryptionKey oldKey = getDekById(keyId);
    // Allow create to act like undelete if the dek is deleted
    if (!rewrap && oldKey != null
        && (request.isDeleted() == oldKey.isDeleted() || !oldKey.isEquivalent(key))) {
      throw new AlreadyExistsException(request.getSubject());
    }
    if (key.getEncryptedKeyMaterial() != null) {
      if (kek.isShared() && oldKey != null) {
        throw new AlreadyExistsException(request.getSubject());
      }
    } else if (kek.isShared()) {
      if (oldKey != null) {
        oldKey = generateRawDek(kek, oldKey);
        key.setKeyMaterial(oldKey.getKeyMaterial());
      }
      key = generateEncryptedDek(kek, key);
    } else {
      throw new InvalidKeyException("encryptedKeyMaterial");
    }
    putKey(keyId, key);
    // Retrieve key with ts set
    key = getDekById(keyId);
    if (kek.isShared()) {
      Mode mode = schemaRegistry.getModeInScope(request.getSubject());
      if (mode != Mode.IMPORT) {
        key = generateRawDek(kek, key);
      }
    }
    return key;
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
    // Ensure store is up-to-date
    syncStore();

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
        removeKey(keyId);
      }
    } else {
      for (KeyValue<EncryptionKeyId, EncryptionKey> dek : deks) {
        if (!dek.value.isDeleted()) {
          DataEncryptionKeyId id = (DataEncryptionKeyId) dek.key;
          DataEncryptionKey oldKey = (DataEncryptionKey) dek.value;
          DataEncryptionKey newKey = new DataEncryptionKey(name, oldKey.getSubject(),
              oldKey.getAlgorithm(), oldKey.getVersion(), oldKey.getEncryptedKeyMaterial(), true);
          putKey(id, newKey);
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
    // Ensure store is up-to-date
    syncStore();

    if (algorithm == null) {
      algorithm = DekFormat.AES256_GCM;
    }
    String tenant = schemaRegistry.tenant();
    DataEncryptionKeyId id = new DataEncryptionKeyId(tenant, name, subject, algorithm, version);
    DataEncryptionKey key = getDekById(id);
    if (key == null) {
      return;
    }
    if (permanentDelete) {
      if (!key.isDeleted()) {
        throw new KeyNotSoftDeletedException(subject);
      }
      removeKey(id);
    } else {
      if (!key.isDeleted()) {
        DataEncryptionKey newKey = new DataEncryptionKey(name, key.getSubject(),
            key.getAlgorithm(), key.getVersion(), key.getEncryptedKeyMaterial(), true);
        putKey(id, newKey);
      }
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
    // Ensure store is up-to-date
    syncStore();

    String tenant = schemaRegistry.tenant();
    KeyEncryptionKeyId keyId = new KeyEncryptionKeyId(tenant, name);
    KeyEncryptionKey key = getKekById(keyId);
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
        putKey(id, newKey);
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
    // Ensure store is up-to-date
    syncStore();

    if (algorithm == null) {
      algorithm = DekFormat.AES256_GCM;
    }
    String tenant = schemaRegistry.tenant();
    KeyEncryptionKeyId keyId = new KeyEncryptionKeyId(tenant, name);
    KeyEncryptionKey key = getKekById(keyId);
    if (key == null) {
      return;
    }
    if (key.isDeleted()) {
      throw new KeySoftDeletedException(name);
    }
    DataEncryptionKeyId id = new DataEncryptionKeyId(tenant, name, subject, algorithm, version);
    DataEncryptionKey oldKey = getDekById(id);
    if (oldKey == null) {
      return;
    }
    if (oldKey.isDeleted()) {
      DataEncryptionKey newKey = new DataEncryptionKey(name, oldKey.getSubject(),
          oldKey.getAlgorithm(), oldKey.getVersion(), oldKey.getEncryptedKeyMaterial(), false);
      putKey(id, newKey);
    }
  }

  // ==================== Encryption/Decryption ====================

  protected DataEncryptionKey generateEncryptedDek(KeyEncryptionKey kek, DataEncryptionKey key)
      throws DekGenerationException {
    try {
      Aead aead = getAead(kek);
      byte[] rawDek = null;
      String rawDekStr = key.getKeyMaterial();
      if (rawDekStr != null) {
        rawDek = Base64.getDecoder().decode(rawDekStr.getBytes(StandardCharsets.UTF_8));
      }
      if (rawDek == null) {
        // Generate new dek
        rawDek = getCryptor(key.getAlgorithm()).generateKey();
      }
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
      Aead aead = getAead(kek);
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

  protected Aead getAead(KeyEncryptionKey kek) throws GeneralSecurityException {
    return toKekEntity(kek).toAead(config.originals());
  }

  // ==================== Limit Normalization ====================

  public int normalizeLimit(int suppliedLimit, int defaultLimit, int maxLimit) {
    int limit = defaultLimit;
    if (suppliedLimit > 0 && suppliedLimit <= maxLimit) {
      limit = suppliedLimit;
    }
    return limit;
  }

  public int normalizeKekLimit(int suppliedLimit) {
    return normalizeLimit(suppliedLimit, kekSearchDefaultLimit, kekSearchMaxLimit);
  }

  public int normalizeDekSubjectLimit(int suppliedLimit) {
    return normalizeLimit(suppliedLimit, dekSubjectSearchDefaultLimit, dekSubjectSearchMaxLimit);
  }

  public int normalizeDekVersionLimit(int suppliedLimit) {
    return normalizeLimit(suppliedLimit, dekVersionSearchDefaultLimit, dekVersionSearchMaxLimit);
  }

  // ==================== Utilities ====================

  protected static byte[] toJson(Object o) throws JsonProcessingException {
    return JacksonMapper.INSTANCE.writeValueAsBytes(o);
  }
}
