/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage;

import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.DEFAULT_CONTEXT;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.ParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.TagSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.rest.utils.UrlList;
import io.confluent.kafka.schemaregistry.exceptions.IdGenerationException;
import io.confluent.kafka.schemaregistry.exceptions.IncompatibleSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.OperationNotPermittedException;
import io.confluent.kafka.schemaregistry.exceptions.ReferenceExistsException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryInitializationException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryRequestForwardingException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryTimeoutException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaTooLargeException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaVersionNotSoftDeletedException;
import io.confluent.kafka.schemaregistry.exceptions.SubjectNotSoftDeletedException;
import io.confluent.kafka.schemaregistry.exceptions.UnknownLeaderException;
import io.confluent.kafka.schemaregistry.id.IdGenerator;
import io.confluent.kafka.schemaregistry.id.IncrementalIdGenerator;
import io.confluent.kafka.schemaregistry.leaderelector.kafka.KafkaGroupLeaderElector;
import io.confluent.kafka.schemaregistry.metrics.MetricsContainer;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.extensions.SchemaRegistryResourceExtension;
import io.confluent.kafka.schemaregistry.storage.encoder.MetadataEncoderService;
import io.confluent.kafka.schemaregistry.storage.exceptions.EntryTooLargeException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreTimeoutException;
import io.confluent.kafka.schemaregistry.storage.serialization.Serializer;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.confluent.rest.NamedURI;
import io.confluent.rest.exceptions.RestException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.apache.avro.reflect.Nullable;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSchemaRegistry extends AbstractSchemaRegistry implements
        LeaderAwareSchemaRegistry {

  private static final Logger log = LoggerFactory.getLogger(KafkaSchemaRegistry.class);
  // visible for testing
  final KafkaStore<SchemaRegistryKey, SchemaRegistryValue> kafkaStore;
  private final Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer;
  private final SchemaRegistryIdentity myIdentity;
  private final int kafkaStoreTimeoutMs;
  private final int initTimeout;
  private final boolean initWaitForReader;
  private final int kafkaStoreMaxRetries;
  private final boolean delayLeaderElection;
  private final boolean enableStoreHealthCheck;
  private SchemaRegistryIdentity leaderIdentity;
  private RestService leaderRestService;
  private final int leaderConnectTimeoutMs;
  private final int leaderReadTimeoutMs;
  private final IdGenerator idGenerator;
  private LeaderElector leaderElector = null;
  private final String kafkaClusterId;
  private final String groupId;
  private final List<Consumer<Boolean>> leaderChangeListeners = new CopyOnWriteArrayList<>();

  public KafkaSchemaRegistry(SchemaRegistryConfig config,
                             Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer)
      throws SchemaRegistryException {
    super(config, initMetricsContainer(config, kafkaClusterId(config)));

    Boolean leaderEligibility = config.getBoolean(SchemaRegistryConfig.MASTER_ELIGIBILITY);
    if (leaderEligibility == null) {
      leaderEligibility = config.getBoolean(SchemaRegistryConfig.LEADER_ELIGIBILITY);
    }
    this.delayLeaderElection = config.getBoolean(SchemaRegistryConfig.LEADER_ELECTION_DELAY);
    this.enableStoreHealthCheck = config.getBoolean(SchemaRegistryConfig.ENABLE_STORE_HEALTH_CHECK);

    String interInstanceListenerNameConfig = config.interInstanceListenerName();
    NamedURI internalListener = getInterInstanceListener(config.getListeners(),
        interInstanceListenerNameConfig,
        config.interInstanceProtocol());
    log.info("Found internal listener: {}", internalListener);
    boolean isEligibleForLeaderElector = leaderEligibility;
    this.myIdentity = getMyIdentity(internalListener, isEligibleForLeaderElector, config);
    log.info("Setting my identity to {}",  myIdentity);

    this.leaderConnectTimeoutMs = config.getInt(SchemaRegistryConfig.LEADER_CONNECT_TIMEOUT_MS);
    this.leaderReadTimeoutMs = config.getInt(SchemaRegistryConfig.LEADER_READ_TIMEOUT_MS);
    this.kafkaStoreTimeoutMs =
        config.getInt(SchemaRegistryConfig.KAFKASTORE_TIMEOUT_CONFIG);
    this.initTimeout = config.getInt(SchemaRegistryConfig.KAFKASTORE_INIT_TIMEOUT_CONFIG);
    this.initWaitForReader =
        config.getBoolean(SchemaRegistryConfig.KAFKASTORE_INIT_WAIT_FOR_READER_CONFIG);
    this.kafkaStoreMaxRetries =
        config.getInt(SchemaRegistryConfig.KAFKASTORE_WRITE_MAX_RETRIES_CONFIG);
    this.serializer = serializer;
    this.kafkaClusterId = kafkaClusterId(config);
    this.groupId = config.getString(SchemaRegistryConfig.SCHEMAREGISTRY_GROUP_ID_CONFIG);
    this.lookupCache = lookupCache();
    this.idGenerator = identityGenerator(config);
    this.kafkaStore = kafkaStore(config);
    this.store = kafkaStore;
    this.metadataEncoder = new MetadataEncoderService(this);
  }

  private static MetricsContainer initMetricsContainer(
      SchemaRegistryConfig config,
      String kafkaClusterId) {
    return new MetricsContainer(config, kafkaClusterId);
  }

  @VisibleForTesting
  static SchemaRegistryIdentity getMyIdentity(NamedURI internalListener,
      boolean isEligibleForLeaderElector, SchemaRegistryConfig config) {
    SchemeAndPort schemeAndPort = new SchemeAndPort(internalListener.getUri().getScheme(),
        // default value of 8081 is always set for `host.port`. only consider `host.port` if the
        // original properties has it. otherwise, use the port from the listener.
        config.originals().containsKey(SchemaRegistryConfig.HOST_PORT_CONFIG)
                ? config.getInt(SchemaRegistryConfig.HOST_PORT_CONFIG) :
                internalListener.getUri().getPort());
    String host = config.getString(SchemaRegistryConfig.HOST_NAME_CONFIG);
    return new SchemaRegistryIdentity(host, schemeAndPort.port, isEligibleForLeaderElector,
        schemeAndPort.scheme);
  }

  protected KafkaStore<SchemaRegistryKey, SchemaRegistryValue> kafkaStore(
      SchemaRegistryConfig config) throws SchemaRegistryException {
    return new KafkaStore<>(
            config,
            getSchemaUpdateHandler(config),
            this.serializer, lookupCache, new NoopKey());
  }

  protected SchemaUpdateHandler getSchemaUpdateHandler(SchemaRegistryConfig config) {
    Map<String, Object> handlerConfigs =
        config.originalsWithPrefix(SchemaRegistryConfig.KAFKASTORE_UPDATE_HANDLERS_CONFIG + ".");
    handlerConfigs.put(StoreUpdateHandler.SCHEMA_REGISTRY, this);
    List<SchemaUpdateHandler> customSchemaHandlers =
        config.getConfiguredInstances(SchemaRegistryConfig.KAFKASTORE_UPDATE_HANDLERS_CONFIG,
            SchemaUpdateHandler.class,
            handlerConfigs);
    KafkaStoreMessageHandler storeHandler =
        new KafkaStoreMessageHandler(this, getLookupCache(), getIdentityGenerator());
    for (SchemaUpdateHandler customSchemaHandler : customSchemaHandlers) {
      log.info("Registering custom schema handler: {}",
          customSchemaHandler.getClass().getName()
      );
    }
    customSchemaHandlers.add(storeHandler);
    return new CompositeSchemaUpdateHandler(customSchemaHandlers);
  }

  protected LookupCache<SchemaRegistryKey, SchemaRegistryValue> lookupCache() {
    return new InMemoryCache<>(serializer);
  }

  public Serializer<SchemaRegistryKey, SchemaRegistryValue> getSerializer() {
    return serializer;
  }

  protected IdGenerator identityGenerator(SchemaRegistryConfig config) {
    config.checkBootstrapServers();
    IdGenerator idGenerator = new IncrementalIdGenerator(this);
    idGenerator.configure(config);
    return idGenerator;
  }

  public IdGenerator getIdentityGenerator() {
    return idGenerator;
  }

  @Override
  public void init() throws SchemaRegistryException {
    try {
      kafkaStore.init();
    } catch (StoreInitializationException e) {
      throw new SchemaRegistryInitializationException(
          "Error initializing kafka store while initializing schema registry", e);
    }
    try {
      metadataEncoder.init();
    } catch (Exception e) {
      throw new SchemaRegistryInitializationException(
          "Error initializing metadata encoder while initializing schema registry", e);
    }

    config.checkBootstrapServers();
    if (!delayLeaderElection) {
      electLeader();
    }
  }

  public void postInit() throws SchemaRegistryException {
    if (delayLeaderElection) {
      electLeader();
    }
    initialized.set(true);
  }

  private void electLeader() throws SchemaRegistryException {
    log.info("Joining schema registry with Kafka-based coordination");
    leaderElector = new KafkaGroupLeaderElector(config, myIdentity, this);
    try {
      leaderElector.init();
    } catch (SchemaRegistryStoreException e) {
      throw new SchemaRegistryInitializationException(
          "Error electing leader while initializing schema registry", e);
    } catch (SchemaRegistryTimeoutException e) {
      throw new SchemaRegistryInitializationException(e);
    }
  }

  public void waitForInit() throws InterruptedException {
    kafkaStore.waitForInit();
  }

  public boolean initialized() {
    return kafkaStore.initialized() && initialized.get();
  }

  public boolean healthy() {
    if (enableStoreHealthCheck) {
      // Get dummy context key
      try {
        // Should return null if key does not exist
        kafkaStore.get(new ContextKey(tenant(), "dummy"));
      } catch (Throwable t) {
        return false;
      }
    }
    return initialized()
        && getResourceExtensions().stream().allMatch(SchemaRegistryResourceExtension::healthy);
  }

  /**
   * Add a leader change listener.
   *
   * @param listener a function that takes whether this node is a leader
   */
  public void addLeaderChangeListener(Consumer<Boolean> listener) {
    leaderChangeListeners.add(listener);
  }

  public boolean isLeader() {
    kafkaStore.leaderLock().lock();
    try {
      return leaderIdentity != null && leaderIdentity.equals(myIdentity);
    } finally {
      kafkaStore.leaderLock().unlock();
    }
  }

  /**
   * 'Inform' this SchemaRegistry instance which SchemaRegistry is the current leader.
   * If this instance is set as the new leader, ensure it is up-to-date with data in
   * the kafka store.
   *
   * @param newLeader Identity of the current leader. null means no leader is alive.
   */
  @Override
  public void setLeader(@Nullable SchemaRegistryIdentity newLeader)
      throws SchemaRegistryTimeoutException, SchemaRegistryStoreException, IdGenerationException {
    final long started = time.hiResClockMs();
    log.info("Setting the leader to {}", newLeader);

    // Only schema registry instances eligible for leader can be set to leader
    if (newLeader != null && !newLeader.getLeaderEligibility()) {
      throw new IllegalStateException(
          "Tried to set an ineligible node to leader: " + newLeader);
    }

    boolean isLeader;
    boolean leaderChanged;
    kafkaStore.leaderLock().lock();
    try {
      final SchemaRegistryIdentity previousLeader = leaderIdentity;
      leaderIdentity = newLeader;

      if (leaderIdentity == null) {
        leaderRestService = null;
      } else {
        leaderRestService = new RestService(leaderIdentity.getUrl(), true);
        leaderRestService.setHttpConnectTimeoutMs(leaderConnectTimeoutMs);
        leaderRestService.setHttpReadTimeoutMs(leaderReadTimeoutMs);
        if (sslFactory != null && sslFactory.sslContext() != null) {
          leaderRestService.setSslSocketFactory(sslFactory.sslContext().getSocketFactory());
          leaderRestService.setHostnameVerifier(getHostnameVerifier());
        }
      }

      isLeader = isLeader();
      leaderChanged = leaderIdentity != null && !leaderIdentity.equals(previousLeader);
      if (leaderChanged) {
        log.info("Leader changed from {} to {}", previousLeader, leaderIdentity);
        if (isLeader) {
          // The new leader may not know the exact last offset in the Kafka log. So, mark the
          // last offset invalid here
          kafkaStore.markLastWrittenOffsetInvalid();
          if (initWaitForReader) {
            //ensure the new leader catches up with the offsets before it gets nextid and assigns
            // leader
            try {
              kafkaStore.waitUntilKafkaReaderReachesLastOffset(initTimeout);
            } catch (StoreException e) {
              throw new SchemaRegistryStoreException("Exception getting latest offset ", e);
            }
          }
          idGenerator.init();
        }
      }
      metricsContainer.getLeaderNode().record(isLeader() ? 1 : 0);
    } finally {
      kafkaStore.leaderLock().unlock();
    }

    if (leaderChanged) {
      for (Consumer<Boolean> listener : leaderChangeListeners) {
        try {
          listener.accept(isLeader);
        } catch (Exception e) {
          log.error("Could not invoke leader change listener", e);
        }
      }
    }
    long elapsed = time.hiResClockMs() - started;
    metricsContainer.getLeaderInitializationLatencyMetric().record(elapsed);
  }

  /**
   * Return json data encoding basic information about this SchemaRegistry instance, such as
   * host, port, etc.
   */
  public SchemaRegistryIdentity myIdentity() {
    return myIdentity;
  }

  /**
   * Return the identity of the SchemaRegistry that this instance thinks is current leader.
   * Any request that requires writing new data gets forwarded to the leader.
   */
  public SchemaRegistryIdentity leaderIdentity() {
    kafkaStore.leaderLock().lock();
    try {
      return leaderIdentity;
    } finally {
      kafkaStore.leaderLock().unlock();
    }
  }

  public RestService leaderRestService() {
    return leaderRestService;
  }

  /**
   * Register the given schema under the given subject.
   *
   * <p>If the schema already exists, it is returned.  During registration, the metadata and ruleSet
   * may be populated by the config that is in scope.</p>
   *
   * @param subject The subject
   * @param schema The schema
   * @param normalize Whether to normalize the schema before registration
   * @return A schema containing the id.  If the schema is different from the input parameter,
   *     it is set in the return object.
   */
  @Override
  public Schema register(String subject,
                         Schema schema,
                         boolean normalize,
                         boolean propagateSchemaTags)
      throws SchemaRegistryException {
    try {
      checkRegisterMode(subject, schema);

      // Ensure cache is up-to-date before any potential writes
      kafkaStore.waitUntilKafkaReaderReachesLastOffset(subject, kafkaStoreTimeoutMs);

      // determine the latest version of the schema in the subject
      List<SchemaKey> allVersions = getAllSchemaKeysDescending(subject);

      List<Schema> deletedVersions = new ArrayList<>();
      List<ParsedSchemaHolder> undeletedVersions = new ArrayList<>();
      int newVersion = MIN_VERSION;
      // iterate from the latest to first
      for (SchemaKey schemaKey : allVersions) {
        LazyParsedSchemaHolder schemaHolder = new LazyParsedSchemaHolder(this, schemaKey);
        SchemaValue schemaValue = schemaHolder.schemaValue();
        newVersion = Math.max(newVersion, schemaValue.getVersion() + 1);
        if (schemaValue.isDeleted()) {
          deletedVersions.add(
              new Schema(schemaValue.getSubject(), schemaValue.getVersion(), schemaValue.getId()));
        } else {
          if (!undeletedVersions.isEmpty()) {
            // minor optimization: clear the holder if it is not the latest
            schemaHolder.clear();
          }
          undeletedVersions.add(schemaHolder);
        }
      }

      Config config = getConfigInScope(subject);
      Mode mode = getModeInScope(subject);

      if (!mode.isImportOrForwardMode()) {
        maybePopulateFromPrevious(
            config, schema, undeletedVersions, newVersion, propagateSchemaTags);
      }

      int schemaId = schema.getId();
      ParsedSchema parsedSchema = canonicalizeSchema(schema, config, schemaId < 0, normalize);

      if (parsedSchema != null) {
        // see if the schema to be registered already exists
        SchemaIdAndSubjects schemaIdAndSubjects = this.lookupCache.schemaIdAndSubjects(schema);
        if (schemaIdAndSubjects != null
            && (schemaId < 0 || schemaId == schemaIdAndSubjects.getSchemaId())) {
          if (schema.getVersion() == 0
              && schemaIdAndSubjects.hasSubject(subject)
              && !isSubjectVersionDeleted(subject, schemaIdAndSubjects.getVersion(subject))) {
            // return only if the schema was previously registered under the input subject
            return schema.copy(
                schemaIdAndSubjects.getVersion(subject), schemaIdAndSubjects.getSchemaId());
          } else {
            // need to register schema under the input subject
            schemaId = schemaIdAndSubjects.getSchemaId();
          }
        }
      }

      // iterate from the latest to first
      if (schema.getVersion() == 0) {
        for (ParsedSchemaHolder schemaHolder : undeletedVersions) {
          SchemaValue schemaValue = ((LazyParsedSchemaHolder) schemaHolder).schemaValue();
          ParsedSchema undeletedSchema = schemaHolder.schema();
          if (parsedSchema != null
              && (schemaId < 0 || schemaId == schemaValue.getId())
              && parsedSchema.canLookup(undeletedSchema, this)) {
            // This handles the case where a schema is sent with all references resolved
            // or without confluent:version
            return schema.copy(schemaValue.getVersion(), schemaValue.getId());
          }
        }
      }

      boolean isCompatible = true;
      List<String> compatibilityErrorLogs = new ArrayList<>();
      if (!mode.isImportOrForwardMode()) {
        // sort undeleted in ascending
        Collections.reverse(undeletedVersions);
        compatibilityErrorLogs.addAll(isCompatibleWithPrevious(config,
            parsedSchema,
            undeletedVersions));
        isCompatible = compatibilityErrorLogs.isEmpty();
      }

      if (isCompatible) {
        // save the context key
        QualifiedSubject qs = QualifiedSubject.create(tenant(), subject);
        if (qs != null && !DEFAULT_CONTEXT.equals(qs.getContext())) {
          ContextKey contextKey = new ContextKey(qs.getTenant(), qs.getContext());
          if (kafkaStore.get(contextKey) == null) {
            ContextValue contextValue = new ContextValue(qs.getTenant(), qs.getContext());
            kafkaStore.put(contextKey, contextValue);
          }
        }

        // assign a guid and put the schema in the kafka store
        if (schema.getVersion() <= 0) {
          schema.setVersion(newVersion);
        } else if (newVersion != schema.getVersion()
                && !mode.isImportOrForwardMode()) {
          throw new InvalidSchemaException("Version is not one more than previous version");
        }

        final SchemaKey schemaKey = new SchemaKey(subject, schema.getVersion());
        final SchemaValue schemaValue = new SchemaValue(schema, ruleSetHandler);
        metadataEncoder.encodeMetadata(schemaValue);
        if (schemaId >= 0) {
          checkIfSchemaWithIdExist(schemaId, schema);
          schema.setId(schemaId);
          schemaValue.setId(schemaId);
        } else {
          String qctx = QualifiedSubject.qualifiedContextFor(tenant(), subject);
          int retries = 0;
          while (retries++ < kafkaStoreMaxRetries) {
            int newId = idGenerator.id(schemaValue);
            // Verify id is not already in use
            if (lookupCache.schemaKeyById(newId, qctx) == null) {
              schema.setId(newId);
              schemaValue.setId(newId);
              if (retries > 1) {
                log.warn("Retrying to register the schema with ID {}", newId);
              }
              break;
            }
          }
          if (retries >= kafkaStoreMaxRetries) {
            throw new SchemaRegistryStoreException("Error while registering the schema due "
                + "to generating an ID that is already in use.");
          }
        }
        for (Schema deleted : deletedVersions) {
          if (deleted.getId().equals(schema.getId())
                  && deleted.getVersion().compareTo(schema.getVersion()) < 0) {
            // Tombstone previous version with the same ID
            SchemaKey key = new SchemaKey(deleted.getSubject(), deleted.getVersion());
            kafkaStore.put(key, null);
          }
        }
        kafkaStore.put(schemaKey, schemaValue);
        logSchemaOp(schema, "REGISTER");
        return schema;
      } else {
        throw new IncompatibleSchemaException(compatibilityErrorLogs.toString());
      }
    } catch (EntryTooLargeException e) {
      throw new SchemaTooLargeException("Write failed because schema is too large", e);
    } catch (StoreTimeoutException te) {
      throw new SchemaRegistryTimeoutException("Write to the Kafka store timed out while", te);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Error while registering the schema in the"
                                             + " backend Kafka store", e);
    } catch (IllegalStateException e) {
      if (e.getCause() instanceof SchemaRegistryException) {
        throw (SchemaRegistryException) e.getCause();
      }
      throw e;
    }
  }

  public Schema registerOrForward(String subject,
                                  RegisterSchemaRequest request,
                                  boolean normalize,
                                  Map<String, String> headerProperties)
      throws SchemaRegistryException {
    Schema schema = new Schema(subject, request);
    Config config = getConfigInScope(subject);
    boolean isLatestVersion = schema.getVersion() == -1;
    if (!request.hasSchemaTagsToAddOrRemove()
        && !request.doPropagateSchemaTags()
        && !config.hasDefaultsOrOverrides()) {
      Schema existingSchema = lookUpSchemaUnderSubject(
          config, subject, schema, normalize, false, isLatestVersion);
      if (existingSchema != null) {
        if (schema.getVersion() == 0 || isLatestVersion) {
          if (schema.getId() == null
              || schema.getId() < 0
              || schema.getId().equals(existingSchema.getId())
          ) {
            return existingSchema;
          }
        } else if (existingSchema.getId().equals(schema.getId())) {
          if (existingSchema.getVersion().equals(schema.getVersion())) {
            return existingSchema;
          } else {
            // In rare cases, a user may have imported the same schema with different versions
            Schema olderVersionSchema = get(subject, schema.getVersion(), false);
            if (olderVersionSchema != null
                && olderVersionSchema.getId().equals(existingSchema.getId())
                && MD5.ofSchema(olderVersionSchema).equals(MD5.ofSchema(existingSchema))) {
              return olderVersionSchema;
            }
          }
        }
      }
    }

    kafkaStore.lockFor(subject).lock();
    try {
      if (isLeader()) {
        return register(subject, request, normalize);
      } else {
        // forward registering request to the leader
        if (leaderIdentity != null) {
          return forwardRegisterRequestToLeader(subject, request, normalize, headerProperties);
        } else {
          throw new UnknownLeaderException("Register schema request failed since leader is "
                                           + "unknown");
        }
      }
    } finally {
      kafkaStore.lockFor(subject).unlock();
    }
  }

  public Schema modifySchemaTagsOrForward(String subject,
                                          Schema schema,
                                          TagSchemaRequest request,
                                          Map<String, String> headerProperties)
      throws SchemaRegistryException {
    kafkaStore.lockFor(subject).lock();
    try {
      if (isLeader()) {
        return modifySchemaTags(subject, schema, request);
      } else {
        // forward registering request to the leader
        if (leaderIdentity != null) {
          return forwardModifySchemaTagsRequestToLeader(
              subject, schema, request, headerProperties);
        } else {
          throw new UnknownLeaderException("Request failed since leader is unknown");
        }
      }
    } finally {
      kafkaStore.lockFor(subject).unlock();
    }
  }

  @Override
  public void deleteSchemaVersion(String subject,
                                  Schema schema,
                                  boolean permanentDelete)
      throws SchemaRegistryException {
    try {
      if (isReadOnlyMode(subject)) {
        String context = QualifiedSubject.qualifiedContextFor(tenant(), subject);
        throw new OperationNotPermittedException("Subject " + subject + " in context "
        + context + " is in read-only mode");
      }
      SchemaKey key = new SchemaKey(subject, schema.getVersion());
      if (!lookupCache.referencesSchema(key).isEmpty()) {
        throw new ReferenceExistsException(key.toString());
      }
      SchemaValue schemaValue = (SchemaValue) lookupCache.get(key);
      if (permanentDelete && schemaValue != null && !schemaValue.isDeleted()) {
        throw new SchemaVersionNotSoftDeletedException(subject, schema.getVersion().toString());
      }
      // Ensure cache is up-to-date before any potential writes
      kafkaStore.waitUntilKafkaReaderReachesLastOffset(subject, kafkaStoreTimeoutMs);
      if (!permanentDelete) {
        schemaValue = new SchemaValue(schema);
        schemaValue.setDeleted(true);
        metadataEncoder.encodeMetadata(schemaValue);
        kafkaStore.put(key, schemaValue);
        logSchemaOp(schema, "DELETE");
      } else {
        kafkaStore.put(key, null);

        if (!getAllVersions(subject, LookupFilter.INCLUDE_DELETED).hasNext()) {
          if (getMode(subject) != null) {
            deleteMode(subject);
          }
          if (getConfig(subject) != null) {
            deleteConfig(subject);
          }
        }
      }
    } catch (StoreTimeoutException te) {
      throw new SchemaRegistryTimeoutException("Write to the Kafka store timed out while", te);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Error while deleting the schema for subject '"
                                            + subject + "' in the backend Kafka store", e);
    }
  }

  public void deleteSchemaVersionOrForward(
      Map<String, String> headerProperties, String subject,
      Schema schema, boolean permanentDelete) throws SchemaRegistryException {

    kafkaStore.lockFor(subject).lock();
    try {
      if (isLeader()) {
        deleteSchemaVersion(subject, schema, permanentDelete);
      } else {
        // forward registering request to the leader
        if (leaderIdentity != null) {
          forwardDeleteSchemaVersionRequestToLeader(headerProperties, subject,
                  schema.getVersion(), permanentDelete);
        } else {
          throw new UnknownLeaderException("Register schema request failed since leader is "
                                           + "unknown");
        }
      }
    } finally {
      kafkaStore.lockFor(subject).unlock();
    }
  }

  @Override
  public List<Integer> deleteSubject(String subject,
                                     boolean permanentDelete) throws SchemaRegistryException {
    // Ensure cache is up-to-date before any potential writes
    try {
      if (isReadOnlyMode(subject)) {
        String context = QualifiedSubject.qualifiedContextFor(tenant(), subject);
        throw new OperationNotPermittedException("Subject " + subject + " in context "
        + context + " is in read-only mode");
      }
      kafkaStore.waitUntilKafkaReaderReachesLastOffset(subject, kafkaStoreTimeoutMs);
      List<Integer> deletedVersions = new ArrayList<>();
      int deleteWatermarkVersion = 0;
      Iterator<SchemaKey> schemasToBeDeleted = getAllVersions(subject,
          permanentDelete ? LookupFilter.INCLUDE_DELETED : LookupFilter.DEFAULT);
      while (schemasToBeDeleted.hasNext()) {
        deleteWatermarkVersion = schemasToBeDeleted.next().getVersion();
        SchemaKey key = new SchemaKey(subject, deleteWatermarkVersion);
        if (!lookupCache.referencesSchema(key).isEmpty()) {
          throw new ReferenceExistsException(key.toString());
        }
        if (permanentDelete) {
          SchemaValue schemaValue = (SchemaValue) lookupCache.get(key);
          if (schemaValue != null && !schemaValue.isDeleted()) {
            throw new SubjectNotSoftDeletedException(subject);
          }
        }
        deletedVersions.add(deleteWatermarkVersion);
      }

      if (!permanentDelete) {
        DeleteSubjectKey key = new DeleteSubjectKey(subject);
        DeleteSubjectValue value = new DeleteSubjectValue(subject, deleteWatermarkVersion);
        kafkaStore.put(key, value);
      } else {
        for (Integer version : deletedVersions) {
          kafkaStore.put(new SchemaKey(subject, version), null);
        }
        if (getMode(subject) != null) {
          deleteMode(subject);
        }
        if (getConfig(subject) != null) {
          deleteConfig(subject);
        }
      }
      return deletedVersions;

    } catch (StoreTimeoutException te) {
      throw new SchemaRegistryTimeoutException("Write to the Kafka store timed out while", te);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Error while deleting the subject in the"
                                             + " backend Kafka store", e);
    }
  }

  public List<Integer> deleteSubjectOrForward(
      Map<String, String> requestProperties,
      String subject,
      boolean permanentDelete) throws SchemaRegistryException {
    kafkaStore.lockFor(subject).lock();
    try {
      if (isLeader()) {
        return deleteSubject(subject, permanentDelete);
      } else {
        // forward registering request to the leader
        if (leaderIdentity != null) {
          return forwardDeleteSubjectRequestToLeader(requestProperties,
                  subject,
                  permanentDelete);
        } else {
          throw new UnknownLeaderException("Register schema request failed since leader is "
                                           + "unknown");
        }
      }
    } finally {
      kafkaStore.lockFor(subject).unlock();
    }
  }

  @Override
  public void deleteContext(String delimitedContext) throws SchemaRegistryException {
    try {
      // Strip the context delimiters
      String rawContext = QualifiedSubject.contextFor(tenant(), delimitedContext);
      ContextKey contextKey = new ContextKey(tenant(), rawContext);
      this.kafkaStore.delete(contextKey);
    } catch (StoreTimeoutException te) {
      throw new SchemaRegistryTimeoutException("Write to the Kafka store timed out while", te);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Error while deleting the context in the"
                                             + " backend Kafka store", e);
    }
  }

  public void deleteContextOrForward(
      Map<String, String> requestProperties,
      String delimitedContext) throws SchemaRegistryException {
    kafkaStore.lockFor(delimitedContext).lock();
    try {
      if (isLeader()) {
        deleteContext(delimitedContext);
      } else {
        // forward registering request to the leader
        if (leaderIdentity != null) {
          forwardDeleteContextRequestToLeader(requestProperties, delimitedContext);
        } else {
          throw new UnknownLeaderException("Register schema request failed since leader is "
                                           + "unknown");
        }
      }
    } finally {
      kafkaStore.lockFor(delimitedContext).unlock();
    }
  }

  private Schema forwardRegisterRequestToLeader(
      String subject, RegisterSchemaRequest registerSchemaRequest, boolean normalize,
      Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    final UrlList baseUrl = leaderRestService.getBaseUrls();

    log.debug("Forwarding registering schema request to {}", baseUrl);
    try {
      RegisterSchemaResponse response = leaderRestService.registerSchema(
          headerProperties, registerSchemaRequest, subject, normalize);
      return new Schema(subject, response);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the registering schema request to %s",
              baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  public Schema forwardModifySchemaTagsRequestToLeader(
      String subject, Schema schema, TagSchemaRequest request, Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {

    final UrlList baseUrl = leaderRestService.getBaseUrls();

    log.debug("Forwarding register schema tags request to {}", baseUrl);
    try {
      RegisterSchemaResponse response = leaderRestService.modifySchemaTags(
          headerProperties, request, subject, String.valueOf(schema.getVersion()));
      return new Schema(subject, response);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the register schema tags request to %s",
              baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  private Config forwardUpdateConfigRequestToLeader(
      String subject, ConfigUpdateRequest configUpdateRequest,
      Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    UrlList baseUrl = leaderRestService.getBaseUrls();
    log.debug("Forwarding update config request {} to {}", configUpdateRequest, baseUrl);
    try {
      return new Config(
          leaderRestService.updateConfig(headerProperties, configUpdateRequest, subject)
      );
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the update config request %s to %s",
                        configUpdateRequest, baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  private void forwardDeleteSchemaVersionRequestToLeader(
      Map<String, String> headerProperties,
      String subject,
      Integer version,
      boolean permanentDelete) throws SchemaRegistryRequestForwardingException {
    UrlList baseUrl = leaderRestService.getBaseUrls();

    log.debug("Forwarding deleteSchemaVersion schema version request {}-{} to {}", subject,
            version, baseUrl);
    try {
      leaderRestService.deleteSchemaVersion(headerProperties, subject,
              String.valueOf(version), permanentDelete);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format(
              "Unexpected error while forwarding deleteSchemaVersion schema version "
              + "request %s-%s to %s", subject, version, baseUrl), e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  private List<Integer> forwardDeleteSubjectRequestToLeader(
      Map<String, String> requestProperties,
      String subject,
      boolean permanentDelete) throws SchemaRegistryRequestForwardingException {
    UrlList baseUrl = leaderRestService.getBaseUrls();

    log.debug("Forwarding delete subject request for {} to {}", subject, baseUrl);
    try {
      return leaderRestService.deleteSubject(requestProperties, subject, permanentDelete);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format(
              "Unexpected error while forwarding delete subject "
              + "request %s to %s", subject, baseUrl), e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  private void forwardDeleteContextRequestToLeader(
      Map<String, String> requestProperties,
      String delimitedContext) throws SchemaRegistryRequestForwardingException {
    UrlList baseUrl = leaderRestService.getBaseUrls();

    log.debug("Forwarding delete context request for {} to {}", delimitedContext, baseUrl);
    try {
      leaderRestService.deleteContext(requestProperties, delimitedContext);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format(
              "Unexpected error while forwarding delete context "
                  + "request %s to %s", delimitedContext, baseUrl), e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  private void forwardDeleteConfigToLeader(
      Map<String, String> requestProperties,
      String subject
  ) throws SchemaRegistryRequestForwardingException {
    UrlList baseUrl = leaderRestService.getBaseUrls();

    log.debug("Forwarding delete subject compatibility config request {} to {}", subject, baseUrl);
    try {
      leaderRestService.deleteConfig(requestProperties, subject);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format(
              "Unexpected error while forwarding delete subject compatibility config"
                  + "request %s to %s", subject, baseUrl), e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  private void forwardSetModeRequestToLeader(
      String subject, ModeUpdateRequest modeUpdateRequest, boolean force,
      Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    UrlList baseUrl = leaderRestService.getBaseUrls();
    log.debug("Forwarding update mode request {} to {}", modeUpdateRequest, baseUrl);
    try {
      leaderRestService.setMode(headerProperties, modeUpdateRequest, subject, force);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the update mode request %s to %s",
              modeUpdateRequest, baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  private void forwardDeleteSubjectModeRequestToLeader(
      String subject,
      Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    UrlList baseUrl = leaderRestService.getBaseUrls();

    log.debug("Forwarding delete subject mode request {} to {}", subject, baseUrl);
    try {
      leaderRestService.deleteSubjectMode(headerProperties, subject);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format(
              "Unexpected error while forwarding delete subject mode"
                  + "request %s to %s", subject, baseUrl), e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  public SchemaString get(
      int id,
      String subject,
      String format,
      boolean fetchMaxId
  ) throws SchemaRegistryException {
    SchemaValue schema;
    try {
      SchemaKey subjectVersionKey = getSchemaKeyUsingContexts(id, subject);
      if (subjectVersionKey == null) {
        return null;
      }
      schema = (SchemaValue) kafkaStore.get(subjectVersionKey);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
          "Error while retrieving schema with id "
          + id
          + " from the backend Kafka"
          + " store", e);
    }
    Schema schemaEntity = toSchemaEntity(schema);
    logSchemaOp(schemaEntity, "READ");
    SchemaString schemaString = subject != null
        ? new SchemaString(schemaEntity)
        : new SchemaString(null, null, schemaEntity);
    if (format != null && !format.trim().isEmpty()) {
      ParsedSchema parsedSchema = parseSchema(schemaEntity, false, false);
      schemaString.setSchemaString(parsedSchema.formattedString(format));
    } else {
      schemaString.setSchemaString(schema.getSchema());
    }
    if (fetchMaxId) {
      schemaString.setMaxId(idGenerator.getMaxId(schema));
    }
    return schemaString;
  }

  @Override
  public void close() throws IOException {
    log.info("Shutting down schema registry");
    if (leaderElector != null) {
      leaderElector.close();
    }
    if (leaderRestService != null) {
      leaderRestService.close();
    }
    kafkaStore.close();
    metadataEncoder.close();
  }

  public Config updateConfig(String subject, ConfigUpdateRequest config)
      throws SchemaRegistryStoreException, OperationNotPermittedException, UnknownLeaderException {
    if (isReadOnlyMode(subject)) {
      String context = QualifiedSubject.qualifiedContextFor(tenant(), subject);
      throw new OperationNotPermittedException("Subject " + subject + " in context "
      + context + " is in read-only mode");
    }
    ConfigKey configKey = new ConfigKey(subject);
    try {
      kafkaStore.waitUntilKafkaReaderReachesLastOffset(subject, kafkaStoreTimeoutMs);
      ConfigValue oldConfig = (ConfigValue) kafkaStore.get(configKey);
      ConfigValue newConfig = ConfigValue.update(subject, oldConfig, config, ruleSetHandler);
      kafkaStore.put(configKey, newConfig);
      log.debug("Wrote new config: {} to the Kafka data store with key {}", config, configKey);
      return newConfig.toConfigEntity();
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Failed to write new config value to the store",
                                             e);
    }
  }

  public Config updateConfigOrForward(String subject, ConfigUpdateRequest newConfig,
                                      Map<String, String> headerProperties)
      throws SchemaRegistryStoreException, SchemaRegistryRequestForwardingException,
      UnknownLeaderException, OperationNotPermittedException {
    kafkaStore.lockFor(subject).lock();
    try {
      if (isLeader()) {
        return updateConfig(subject, newConfig);
      } else {
        // forward update config request to the leader
        if (leaderIdentity != null) {
          return forwardUpdateConfigRequestToLeader(subject, newConfig, headerProperties);
        } else {
          throw new UnknownLeaderException("Update config request failed since leader is "
                                           + "unknown");
        }
      }
    } finally {
      kafkaStore.lockFor(subject).unlock();
    }
  }

  public void deleteSubjectConfig(String subject)
      throws SchemaRegistryStoreException, OperationNotPermittedException {
    if (isReadOnlyMode(subject)) {
      String context = QualifiedSubject.qualifiedContextFor(tenant(), subject);
      throw new OperationNotPermittedException("Subject " + subject + " in context "
      + context + " is in read-only mode");
    }
    try {
      kafkaStore.waitUntilKafkaReaderReachesLastOffset(subject, kafkaStoreTimeoutMs);
      deleteConfig(subject);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Failed to delete subject config value from store",
          e);
    }
  }

  public void deleteConfigOrForward(String subject,
                                    Map<String, String> headerProperties)
      throws SchemaRegistryStoreException, SchemaRegistryRequestForwardingException,
      OperationNotPermittedException, UnknownLeaderException {
    kafkaStore.lockFor(subject).lock();
    try {
      if (isLeader()) {
        deleteSubjectConfig(subject);
      } else {
        // forward delete subject config request to the leader
        if (leaderIdentity != null) {
          forwardDeleteConfigToLeader(headerProperties, subject);
        } else {
          throw new UnknownLeaderException("Delete config request failed since leader is "
              + "unknown");
        }
      }
    } finally {
      kafkaStore.lockFor(subject).unlock();
    }
  }

  private static String kafkaClusterId(SchemaRegistryConfig config)
      throws SchemaRegistryException {
    int initTimeout = config.getInt(SchemaRegistryConfig.KAFKASTORE_INIT_TIMEOUT_CONFIG);
    Properties adminClientProps = new Properties();
    KafkaStore.addSchemaRegistryConfigsToClientProperties(config, adminClientProps);
    adminClientProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapBrokers());

    try (AdminClient adminClient = AdminClient.create(adminClientProps)) {
      return adminClient
              .describeCluster()
              .clusterId()
              .get(initTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new SchemaRegistryException("Failed to get Kafka cluster ID", e);
    }
  }

  public String getKafkaClusterId() {
    return kafkaClusterId;
  }

  public String getGroupId() {
    return groupId;
  }

  private void deleteMode(String subject) throws StoreException {
    ModeKey modeKey = new ModeKey(subject);
    this.kafkaStore.delete(modeKey);
  }

  private void deleteConfig(String subject) throws StoreException {
    ConfigKey configKey = new ConfigKey(subject);
    this.kafkaStore.delete(configKey);
  }

  public void setMode(String subject, ModeUpdateRequest request, boolean force)
      throws SchemaRegistryException {
    if (!allowModeChanges) {
      throw new OperationNotPermittedException("Mode changes are not allowed");
    }
    Mode mode = null;
    if (request.getOptionalMode().isPresent()) {
      mode = Enum.valueOf(io.confluent.kafka.schemaregistry.storage.Mode.class,
          request.getMode().toUpperCase(Locale.ROOT));
    }
    ModeKey modeKey = new ModeKey(subject);
    try {
      kafkaStore.waitUntilKafkaReaderReachesLastOffset(subject, kafkaStoreTimeoutMs);
      if (mode == Mode.IMPORT && getModeInScope(subject) != Mode.IMPORT && !force) {
        // Changing to import mode requires that no schemas exist with matching subjects.
        if (hasSubjects(subject, false)) {
          throw new OperationNotPermittedException("Cannot import since found existing subjects");
        }

        // Hard delete any remaining versions
        List<SchemaKey> deletedVersions = new ArrayList<>();
        Set<String> allSubjects = subjects(subject, true);
        for (String s : allSubjects) {
          Iterator<SchemaKey> schemasToBeDeleted = getAllVersions(s, LookupFilter.INCLUDE_DELETED);
          while (schemasToBeDeleted.hasNext()) {
            SchemaKey key = schemasToBeDeleted.next();
            if (!lookupCache.referencesSchema(key).isEmpty()) {
              throw new ReferenceExistsException(key.toString());
            }
            deletedVersions.add(key);
          }
        }
        for (SchemaKey key : deletedVersions) {
          kafkaStore.put(key, null);
        }

        // At this point no schemas should exist with matching subjects.
        // Write an event to clear deleted schemas from the caches.
        kafkaStore.put(new ClearSubjectKey(subject), new ClearSubjectValue(subject));
      }
      kafkaStore.put(modeKey, mode != null ? new ModeValue(subject, mode) : null);
      log.debug("Wrote new mode: {} to the Kafka data store with key {}", mode, modeKey);
    } catch (StoreTimeoutException te) {
      throw new SchemaRegistryTimeoutException("Write to the Kafka store timed out while", te);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Failed to write new mode to the store", e);
    }
  }

  public void setModeOrForward(String subject, ModeUpdateRequest mode, boolean force,
      Map<String, String> headerProperties) throws SchemaRegistryException {
    kafkaStore.lockFor(subject).lock();
    try {
      if (isLeader()) {
        setMode(subject, mode, force);
      } else {
        // forward update mode request to the leader
        if (leaderIdentity != null) {
          forwardSetModeRequestToLeader(subject, mode, force, headerProperties);
        } else {
          throw new UnknownLeaderException("Update mode request failed since leader is "
              + "unknown");
        }
      }
    } finally {
      kafkaStore.lockFor(subject).unlock();
    }
  }

  public void deleteSubjectMode(String subject)
      throws SchemaRegistryStoreException, OperationNotPermittedException {
    if (!allowModeChanges) {
      throw new OperationNotPermittedException("Mode changes are not allowed");
    }
    try {
      kafkaStore.waitUntilKafkaReaderReachesLastOffset(subject, kafkaStoreTimeoutMs);
      deleteMode(subject);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Failed to delete subject config value from store",
          e);
    }
  }

  public void deleteSubjectModeOrForward(String subject, Map<String, String> headerProperties)
      throws SchemaRegistryStoreException, SchemaRegistryRequestForwardingException,
      OperationNotPermittedException, UnknownLeaderException {
    kafkaStore.lockFor(subject).lock();
    try {
      if (isLeader()) {
        deleteSubjectMode(subject);
      } else {
        // forward delete subject config request to the leader
        if (leaderIdentity != null) {
          forwardDeleteSubjectModeRequestToLeader(subject, headerProperties);
        } else {
          throw new UnknownLeaderException("Delete config request failed since leader is "
              + "unknown");
        }
      }
    } finally {
      kafkaStore.lockFor(subject).unlock();
    }
  }

  @Override
  public KafkaStore<SchemaRegistryKey, SchemaRegistryValue> getKafkaStore() {
    return this.kafkaStore;
  }
}
