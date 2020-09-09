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

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.rest.utils.UrlList;
import io.confluent.kafka.schemaregistry.client.security.SslFactory;
import io.confluent.kafka.schemaregistry.exceptions.IdDoesNotMatchException;
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
import io.confluent.kafka.schemaregistry.exceptions.SchemaVersionNotSoftDeletedException;
import io.confluent.kafka.schemaregistry.exceptions.SubjectNotSoftDeletedException;
import io.confluent.kafka.schemaregistry.exceptions.UnknownLeaderException;
import io.confluent.kafka.schemaregistry.id.IdGenerator;
import io.confluent.kafka.schemaregistry.id.IncrementalIdGenerator;
import io.confluent.kafka.schemaregistry.id.ZookeeperIdGenerator;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.leaderelector.kafka.KafkaGroupLeaderElector;
import io.confluent.kafka.schemaregistry.leaderelector.zookeeper.ZookeeperLeaderElector;
import io.confluent.kafka.schemaregistry.metrics.MetricsContainer;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.VersionId;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreTimeoutException;
import io.confluent.kafka.schemaregistry.storage.serialization.Serializer;
import io.confluent.rest.Application;
import io.confluent.rest.RestConfig;
import io.confluent.rest.exceptions.RestException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.apache.avro.reflect.Nullable;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class KafkaSchemaRegistry implements SchemaRegistry, LeaderAwareSchemaRegistry {

  /**
   * Schema versions under a particular subject are indexed from MIN_VERSION.
   */
  public static final int MIN_VERSION = 1;
  public static final int MAX_VERSION = Integer.MAX_VALUE;
  private static final Logger log = LoggerFactory.getLogger(KafkaSchemaRegistry.class);

  private static final long DESCRIBE_CLUSTER_TIMEOUT_MS = 10000L;

  private final SchemaRegistryConfig config;
  private final Map<String, Object> props;
  private final LookupCache<SchemaRegistryKey, SchemaRegistryValue> lookupCache;
  // visible for testing
  final KafkaStore<SchemaRegistryKey, SchemaRegistryValue> kafkaStore;
  private final Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer;
  private final SchemaRegistryIdentity myIdentity;
  private final CompatibilityLevel defaultCompatibilityLevel;
  private final Mode defaultMode;
  private final int kafkaStoreTimeoutMs;
  private final int initTimeout;
  private final int kafkaStoreMaxRetries;
  private final boolean isEligibleForLeaderElector;
  private final boolean allowModeChanges;
  private SchemaRegistryIdentity leaderIdentity;
  private RestService leaderRestService;
  private SslFactory sslFactory;
  private IdGenerator idGenerator = null;
  private LeaderElector leaderElector = null;
  private final MetricsContainer metricsContainer;
  private final Map<String, SchemaProvider> providers;
  private final String kafkaClusterId;

  public KafkaSchemaRegistry(SchemaRegistryConfig config,
                             Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer)
      throws SchemaRegistryException {
    if (config == null) {
      throw new SchemaRegistryException("Schema registry configuration is null");
    }
    this.config = config;
    this.props = new ConcurrentHashMap<>();
    Boolean leaderEligibility = config.getBoolean(SchemaRegistryConfig.MASTER_ELIGIBILITY);
    if (leaderEligibility == null) {
      leaderEligibility = config.getBoolean(SchemaRegistryConfig.LEADER_ELIGIBILITY);
    }
    this.isEligibleForLeaderElector = leaderEligibility;
    String host = config.getString(SchemaRegistryConfig.HOST_NAME_CONFIG);
    SchemeAndPort schemeAndPort = getSchemeAndPortForIdentity(
        config.getInt(SchemaRegistryConfig.PORT_CONFIG),
        config.getList(RestConfig.LISTENERS_CONFIG),
        config.interInstanceProtocol()
    );
    this.allowModeChanges = config.getBoolean(SchemaRegistryConfig.MODE_MUTABILITY);
    this.myIdentity = new SchemaRegistryIdentity(host, schemeAndPort.port,
        isEligibleForLeaderElector, schemeAndPort.scheme);
    this.sslFactory =
        new SslFactory(ConfigDef.convertToStringMapWithPasswordValues(config.values()));
    this.kafkaStoreTimeoutMs =
        config.getInt(SchemaRegistryConfig.KAFKASTORE_TIMEOUT_CONFIG);
    this.initTimeout = config.getInt(SchemaRegistryConfig.KAFKASTORE_INIT_TIMEOUT_CONFIG);
    this.kafkaStoreMaxRetries =
        config.getInt(SchemaRegistryConfig.KAFKASTORE_WRITE_MAX_RETRIES_CONFIG);
    this.serializer = serializer;
    this.defaultCompatibilityLevel = config.compatibilityType();
    this.defaultMode = Mode.READWRITE;
    this.kafkaClusterId = kafkaClusterId(config);
    this.metricsContainer = new MetricsContainer(config, this.kafkaClusterId);
    this.providers = initProviders(config);
    this.lookupCache = lookupCache();
    this.idGenerator = identityGenerator(config);
    this.kafkaStore = kafkaStore(config);
  }

  private Map<String, SchemaProvider> initProviders(SchemaRegistryConfig config) {
    Map<String, Object> schemaProviderConfigs =
        config.originalsWithPrefix(SchemaRegistryConfig.SCHEMA_PROVIDERS_CONFIG + ".");
    schemaProviderConfigs.put(SchemaProvider.SCHEMA_VERSION_FETCHER_CONFIG, this);
    List<SchemaProvider> defaultSchemaProviders = Arrays.asList(
        new AvroSchemaProvider(), new JsonSchemaProvider(), new ProtobufSchemaProvider()
    );
    for (SchemaProvider provider : defaultSchemaProviders) {
      provider.configure(schemaProviderConfigs);
    }
    Map<String, SchemaProvider> providerMap = new HashMap<>();
    registerProviders(providerMap, defaultSchemaProviders);
    List<SchemaProvider> customSchemaProviders =
        config.getConfiguredInstances(SchemaRegistryConfig.SCHEMA_PROVIDERS_CONFIG,
            SchemaProvider.class,
            schemaProviderConfigs);
    // Allow custom providers to override default providers
    registerProviders(providerMap, customSchemaProviders);
    metricsContainer.getCustomSchemaProviderCount().set(customSchemaProviders.size());
    return providerMap;
  }

  private void registerProviders(
      Map<String, SchemaProvider> providerMap,
      List<SchemaProvider> schemaProviders
  ) {
    for (SchemaProvider schemaProvider : schemaProviders) {
      log.info("Registering schema provider for {}: {}",
          schemaProvider.schemaType(),
          schemaProvider.getClass().getName()
      );
      providerMap.put(schemaProvider.schemaType(), schemaProvider);
    }
  }

  protected KafkaStore<SchemaRegistryKey, SchemaRegistryValue> kafkaStore(
      SchemaRegistryConfig config) throws SchemaRegistryException {
    return new KafkaStore<SchemaRegistryKey, SchemaRegistryValue>(
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
    return new InMemoryCache<SchemaRegistryKey, SchemaRegistryValue>(serializer);
  }

  public LookupCache<SchemaRegistryKey, SchemaRegistryValue> getLookupCache() {
    return lookupCache;
  }

  public Serializer<SchemaRegistryKey, SchemaRegistryValue> getSerializer() {
    return serializer;
  }

  protected IdGenerator identityGenerator(SchemaRegistryConfig config) {
    IdGenerator idGenerator;
    if (config.useKafkaCoordination()) {
      idGenerator = new IncrementalIdGenerator();
    } else {
      idGenerator = new ZookeeperIdGenerator();
    }
    idGenerator.configure(config);
    return idGenerator;
  }

  public IdGenerator getIdentityGenerator() {
    return idGenerator;
  }

  public MetricsContainer getMetricsContainer() {
    return metricsContainer;
  }

  /**
   * A Schema Registry instance's identity is in part the port it listens on. Currently the port can
   * either be configured via the deprecated `port` configuration, or via the `listeners`
   * configuration.
   *
   * <p>This method uses `Application.parseListeners()` from `rest-utils` to get a list of
   * listeners, and returns the port of the first listener to be used for the instance's identity.
   *
   * <p></p>In theory, any port from any listener would be sufficient. Choosing the first, instead
   * of say the last, is arbitrary.
   */
  // TODO: once RestConfig.PORT_CONFIG is deprecated, remove the port parameter.
  static SchemeAndPort getSchemeAndPortForIdentity(int port, List<String> configuredListeners,
                                                   String requestedScheme)
      throws SchemaRegistryException {
    List<URI> listeners = Application.parseListeners(configuredListeners, port,
                                                     Arrays.asList(
                                                         SchemaRegistryConfig.HTTP,
                                                         SchemaRegistryConfig.HTTPS
                                                     ), SchemaRegistryConfig.HTTP
    );
    if (requestedScheme.isEmpty()) {
      requestedScheme = SchemaRegistryConfig.HTTP;
    }
    for (URI listener: listeners) {
      if (requestedScheme.equalsIgnoreCase(listener.getScheme())) {
        return new SchemeAndPort(listener.getScheme(), listener.getPort());
      }
    }
    throw new SchemaRegistryException(" No listener configured with requested scheme "
                                      + requestedScheme);
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
      if (config.useKafkaCoordination()) {
        log.info("Joining schema registry with Kafka-based coordination");
        leaderElector = new KafkaGroupLeaderElector(config, myIdentity, this);
      } else {
        log.info("Joining schema registry with Zookeeper-based coordination");
        log.warn("*****************************************************************************");
        log.warn("Zookeeper-based coordination is deprecated and will be removed in the future.");
        log.warn("Please switch to Kafka-based coordination with "
            + "\"kafkastore.bootstrap.servers\".");
        log.warn("*****************************************************************************");
        leaderElector = new ZookeeperLeaderElector(config, myIdentity, this);
      }
      leaderElector.init();
    } catch (SchemaRegistryStoreException e) {
      throw new SchemaRegistryInitializationException(
          "Error electing leader while initializing schema registry", e);
    } catch (SchemaRegistryTimeoutException e) {
      throw new SchemaRegistryInitializationException(e);
    }
  }

  public boolean initialized() {
    return kafkaStore.initialized();
  }

  public boolean isLeader() {
    kafkaStore.leaderLock().lock();
    try {
      if (leaderIdentity != null && leaderIdentity.equals(myIdentity)) {
        return true;
      } else {
        return false;
      }
    } finally {
      kafkaStore.leaderLock().unlock();
    }
  }

  /**
   * 'Inform' this SchemaRegistry instance which SchemaRegistry is the current leader.
   * If this instance is set as the new leader, ensure it is up-to-date with data in
   * the kafka store, and tell Zookeeper to allocate the next batch of schema IDs.
   *
   * @param newLeader Identity of the current leader. null means no leader is alive.
   */
  @Override
  public void setLeader(@Nullable SchemaRegistryIdentity newLeader)
      throws SchemaRegistryTimeoutException, SchemaRegistryStoreException, IdGenerationException {
    log.debug("Setting the leader to " + newLeader);

    // Only schema registry instances eligible for leader can be set to leader
    if (newLeader != null && !newLeader.getLeaderEligibility()) {
      throw new IllegalStateException(
          "Tried to set an ineligible node to leader: " + newLeader);
    }

    kafkaStore.leaderLock().lock();
    try {
      SchemaRegistryIdentity previousLeader = leaderIdentity;
      leaderIdentity = newLeader;

      if (leaderIdentity == null) {
        leaderRestService = null;
      } else {
        leaderRestService = new RestService(leaderIdentity.getUrl());
        if (sslFactory != null && sslFactory.sslContext() != null) {
          leaderRestService.setSslSocketFactory(sslFactory.sslContext().getSocketFactory());
          leaderRestService.setHostnameVerifier(getHostnameVerifier());
        }
      }

      if (leaderIdentity != null && !leaderIdentity.equals(previousLeader) && isLeader()) {
        // The new leader may not know the exact last offset in the Kafka log. So, mark the
        // last offset invalid here
        kafkaStore.markLastWrittenOffsetInvalid();
        //ensure the new leader catches up with the offsets before it gets nextid and assigns
        // leader
        try {
          kafkaStore.waitUntilKafkaReaderReachesLastOffset(initTimeout);
        } catch (StoreException e) {
          throw new SchemaRegistryStoreException("Exception getting latest offset ", e);
        }
        idGenerator.init();
      }
      metricsContainer.getLeaderNode().set(isLeader() ? 1 : 0);
    } finally {
      kafkaStore.leaderLock().unlock();
    }
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

  public Set<String> schemaTypes() {
    return providers.keySet();
  }

  @Override
  public int register(String subject,
                      Schema schema)
      throws SchemaRegistryException {
    try {
      checkRegisterMode(subject, schema);

      // Ensure cache is up-to-date before any potential writes
      kafkaStore.waitUntilKafkaReaderReachesLastOffset(subject, kafkaStoreTimeoutMs);

      ParsedSchema parsedSchema = canonicalizeSchema(schema);

      // see if the schema to be registered already exists
      int schemaId = schema.getId();
      SchemaIdAndSubjects schemaIdAndSubjects = this.lookupCache.schemaIdAndSubjects(schema);
      if (schemaIdAndSubjects != null) {
        if (schemaId >= 0 && schemaId != schemaIdAndSubjects.getSchemaId()) {
          throw new IdDoesNotMatchException(schemaIdAndSubjects.getSchemaId(), schema.getId());
        }
        if (schemaIdAndSubjects.hasSubject(subject)
            && !isSubjectVersionDeleted(subject, schemaIdAndSubjects.getVersion(subject))) {
          // return only if the schema was previously registered under the input subject
          return schemaIdAndSubjects.getSchemaId();
        } else {
          // need to register schema under the input subject
          schemaId = schemaIdAndSubjects.getSchemaId();
        }
      }

      // determine the latest version of the schema in the subject
      List<SchemaValue> allVersions = getAllSchemaValues(subject);
      Collections.reverse(allVersions);

      List<SchemaValue> deletedVersions = new ArrayList<>();
      List<ParsedSchema> undeletedVersions = new ArrayList<>();
      int newVersion = MIN_VERSION;
      for (SchemaValue schemaValue : allVersions) {
        newVersion = Math.max(newVersion, schemaValue.getVersion() + 1);
        if (schemaValue.isDeleted()) {
          deletedVersions.add(schemaValue);
        } else {
          ParsedSchema undeletedSchema = parseSchema(getSchemaEntityFromSchemaValue(schemaValue));
          if (parsedSchema.references().isEmpty()
              && !undeletedSchema.references().isEmpty()
              && parsedSchema.deepEquals(undeletedSchema)) {
            // This handles the case where a schema is sent with all references resolved
            return schemaValue.getId();
          }
          undeletedVersions.add(undeletedSchema);
        }
      }
      Collections.reverse(undeletedVersions);

      boolean isCompatible =
              isCompatibleWithPrevious(subject, parsedSchema, undeletedVersions).isEmpty();
      // Allow schema providers to modify the schema during compatibility checks
      schema.setSchema(parsedSchema.canonicalString());
      schema.setReferences(parsedSchema.references());

      if (isCompatible) {
        // assign a guid and put the schema in the kafka store
        if (schema.getVersion() <= 0) {
          schema.setVersion(newVersion);
        }

        SchemaKey schemaKey = new SchemaKey(subject, schema.getVersion());
        if (schemaId >= 0) {
          schema.setId(schemaId);
          kafkaStore.put(schemaKey, new SchemaValue(schema));
        } else {
          int retries = 0;
          while (retries++ < kafkaStoreMaxRetries) {
            int newId = idGenerator.id(schema);
            // Verify id is not already in use
            if (lookupCache.schemaKeyById(newId) == null) {
              schema.setId(newId);
              if (retries > 1) {
                log.warn(String.format("Retrying to register the schema with ID %s", newId));
              }
              kafkaStore.put(schemaKey, new SchemaValue(schema));
              break;
            }
          }
          if (retries >= kafkaStoreMaxRetries) {
            throw new SchemaRegistryStoreException("Error while registering the schema due "
                + "to generating an ID that is already in use.");
          }
        }
        for (SchemaValue schemaValue : deletedVersions) {
          if (schemaValue.getId().equals(schema.getId())) {
            // Tombstone previous version with the same ID
            SchemaKey key = new SchemaKey(schemaValue.getSubject(), schemaValue.getVersion());
            kafkaStore.delete(key);
          }
        }
        return schema.getId();
      } else {
        throw new IncompatibleSchemaException(
            "New schema is incompatible with an earlier schema.");
      }
    } catch (StoreTimeoutException te) {
      throw new SchemaRegistryTimeoutException("Write to the Kafka store timed out while", te);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Error while registering the schema in the"
                                             + " backend Kafka store", e);
    }
  }

  private void checkRegisterMode(
      String subject, Schema schema
  ) throws OperationNotPermittedException, SchemaRegistryStoreException {
    if (getModeInScope(subject) == Mode.READONLY) {
      throw new OperationNotPermittedException("Subject " + subject + " is in read-only mode");
    }

    if (schema.getId() >= 0 || schema.getVersion() > 0) {
      if (getModeInScope(subject) != Mode.IMPORT) {
        throw new OperationNotPermittedException("Subject " + subject + " is not in import mode");
      }
    } else {
      if (getModeInScope(subject) != Mode.READWRITE) {
        throw new OperationNotPermittedException(
            "Subject " + subject + " is not in read-write mode"
        );
      }
    }
  }

  public int registerOrForward(String subject,
                               Schema schema,
                               Map<String, String> headerProperties)
      throws SchemaRegistryException {
    Schema existingSchema = lookUpSchemaUnderSubject(subject, schema, false);
    if (existingSchema != null) {
      if (schema.getId() != null
          && schema.getId() >= 0
          && !schema.getId().equals(existingSchema.getId())
      ) {
        throw new IdDoesNotMatchException(existingSchema.getId(), schema.getId());
      }
      return existingSchema.getId();
    }

    kafkaStore.lockFor(subject).lock();
    try {
      if (isLeader()) {
        return register(subject, schema);
      } else {
        // forward registering request to the leader
        if (leaderIdentity != null) {
          return forwardRegisterRequestToLeader(subject, schema, headerProperties);
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
  public void deleteSchemaVersion(String subject,
                                  Schema schema,
                                  boolean permanentDelete)
      throws SchemaRegistryException {
    try {
      if (getModeInScope(subject) == Mode.READONLY) {
        throw new OperationNotPermittedException("Subject " + subject + " is in read-only mode");
      }
      SchemaKey key = new SchemaKey(subject, schema.getVersion());
      if (!lookupCache.referencesSchema(key).isEmpty()) {
        throw new ReferenceExistsException(key.toString());
      }
      SchemaValue schemaValue = (SchemaValue) lookupCache.get(key);
      if (permanentDelete && !schemaValue.isDeleted()) {
        throw new SchemaVersionNotSoftDeletedException(subject, schema.getVersion().toString());
      }
      // Ensure cache is up-to-date before any potential writes
      kafkaStore.waitUntilKafkaReaderReachesLastOffset(subject, kafkaStoreTimeoutMs);
      if (!permanentDelete) {
        schemaValue = new SchemaValue(schema);
        schemaValue.setDeleted(true);
        kafkaStore.put(key, schemaValue);
        if (!getAllVersions(subject, false).hasNext()) {
          if (getMode(subject) != null) {
            deleteMode(subject);
          }
          if (getCompatibilityLevel(subject) != null) {
            deleteSubjectCompatibility(subject);
          }
        }
      } else {

        kafkaStore.put(key, null);
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
      if (getModeInScope(subject) == Mode.READONLY) {
        throw new OperationNotPermittedException("Subject " + subject + " is in read-only mode");
      }
      kafkaStore.waitUntilKafkaReaderReachesLastOffset(subject, kafkaStoreTimeoutMs);
      List<Integer> deletedVersions = new ArrayList<>();
      int deleteWatermarkVersion = 0;
      Iterator<Schema> schemasToBeDeleted = getAllVersions(subject, permanentDelete);
      while (schemasToBeDeleted.hasNext()) {
        deleteWatermarkVersion = schemasToBeDeleted.next().getVersion();
        SchemaKey key = new SchemaKey(subject, deleteWatermarkVersion);
        if (!lookupCache.referencesSchema(key).isEmpty()) {
          throw new ReferenceExistsException(key.toString());
        }
        if (permanentDelete) {
          SchemaValue schemaValue = (SchemaValue) lookupCache.get(key);
          if (!schemaValue.isDeleted()) {
            throw new SubjectNotSoftDeletedException(subject);
          }
        }
        deletedVersions.add(deleteWatermarkVersion);
      }

      if (!permanentDelete) {
        DeleteSubjectKey key = new DeleteSubjectKey(subject);
        DeleteSubjectValue value = new DeleteSubjectValue(subject, deleteWatermarkVersion);
        kafkaStore.put(key, value);
        if (getMode(subject) != null) {
          deleteMode(subject);
        }
        if (getCompatibilityLevel(subject) != null) {
          deleteSubjectCompatibility(subject);
        }
      } else {
        for (Integer version : deletedVersions) {
          kafkaStore.put(new SchemaKey(subject, version), null);
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


  /**
   * Checks if given schema was ever registered under a subject. If found, it returns the version of
   * the schema under the subject. If not, returns -1
   */
  public Schema lookUpSchemaUnderSubject(String subject, Schema schema, boolean lookupDeletedSchema)
      throws SchemaRegistryException {
    try {
      ParsedSchema parsedSchema = canonicalizeSchema(schema);
      SchemaIdAndSubjects schemaIdAndSubjects = this.lookupCache.schemaIdAndSubjects(schema);
      if (schemaIdAndSubjects != null) {
        if (schemaIdAndSubjects.hasSubject(subject)
            && (lookupDeletedSchema || !isSubjectVersionDeleted(subject, schemaIdAndSubjects
            .getVersion(subject)))) {
          Schema matchingSchema = new Schema(subject,
                                             schemaIdAndSubjects.getVersion(subject),
                                             schemaIdAndSubjects.getSchemaId(),
                                             schema.getSchemaType(),
                                             schema.getReferences(),
                                             schema.getSchema());
          return matchingSchema;
        }
      }

      List<SchemaValue> allVersions = getAllSchemaValues(subject);
      Collections.reverse(allVersions);

      for (SchemaValue schemaValue : allVersions) {
        if (!schemaValue.isDeleted()
            && parsedSchema.references().isEmpty()
            && !schemaValue.getReferences().isEmpty()) {
          Schema undeleted = getSchemaEntityFromSchemaValue(schemaValue);
          ParsedSchema undeletedSchema = parseSchema(undeleted);
          if (parsedSchema.deepEquals(undeletedSchema)) {
            // This handles the case where a schema is sent with all references resolved
            return undeleted;
          }
        }
      }

      return null;
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
          "Error from the backend Kafka store", e);
    }
  }

  private int forwardRegisterRequestToLeader(String subject, Schema schema,
                                             Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    final UrlList baseUrl = leaderRestService.getBaseUrls();

    RegisterSchemaRequest registerSchemaRequest = new RegisterSchemaRequest();
    registerSchemaRequest.setSchema(schema.getSchema());
    registerSchemaRequest.setSchemaType(schema.getSchemaType());
    registerSchemaRequest.setReferences(schema.getReferences());
    registerSchemaRequest.setVersion(schema.getVersion());
    registerSchemaRequest.setId(schema.getId());
    log.debug(String.format("Forwarding registering schema request %s to %s",
                            registerSchemaRequest, baseUrl));
    try {
      int id = leaderRestService.registerSchema(headerProperties, registerSchemaRequest, subject);
      return id;
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the registering schema request %s to %s",
                        registerSchemaRequest, baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  private void forwardUpdateCompatibilityLevelRequestToLeader(
      String subject, CompatibilityLevel compatibilityLevel,
      Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    UrlList baseUrl = leaderRestService.getBaseUrls();

    ConfigUpdateRequest configUpdateRequest = new ConfigUpdateRequest();
    configUpdateRequest.setCompatibilityLevel(compatibilityLevel.name);
    log.debug(String.format("Forwarding update config request %s to %s",
                            configUpdateRequest, baseUrl));
    try {
      leaderRestService.updateConfig(headerProperties, configUpdateRequest, subject);
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

    log.debug(String.format("Forwarding deleteSchemaVersion schema version request %s-%s to %s",
                            subject, version, baseUrl));
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

    log.debug(String.format("Forwarding delete subject request for  %s to %s",
                            subject, baseUrl));
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

  private void forwardSetModeRequestToLeader(
      String subject, Mode mode,
      Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    UrlList baseUrl = leaderRestService.getBaseUrls();

    ModeUpdateRequest modeUpdateRequest = new ModeUpdateRequest();
    modeUpdateRequest.setMode(mode.name());
    log.debug(String.format("Forwarding update mode request %s to %s",
        modeUpdateRequest, baseUrl));
    try {
      leaderRestService.setMode(headerProperties, modeUpdateRequest, subject);
    } catch (IOException e) {
      throw new SchemaRegistryRequestForwardingException(
          String.format("Unexpected error while forwarding the update mode request %s to %s",
              modeUpdateRequest, baseUrl),
          e);
    } catch (RestClientException e) {
      throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
    }
  }

  private ParsedSchema canonicalizeSchema(Schema schema) throws InvalidSchemaException {
    if (schema == null
        || schema.getSchema() == null
        || schema.getSchema().trim().isEmpty()) {
      log.error("Empty schema");
      throw new InvalidSchemaException("Empty schema");
    }
    ParsedSchema parsedSchema = parseSchema(schema);
    try {
      parsedSchema.validate();
    } catch (Exception e) {
      String errMsg = "Invalid schema " + schema;
      log.error(errMsg, e);
      throw new InvalidSchemaException(errMsg, e);
    }
    schema.setSchema(parsedSchema.canonicalString());
    schema.setReferences(parsedSchema.references());
    return parsedSchema;
  }

  private ParsedSchema parseSchema(Schema schema) throws InvalidSchemaException {
    return parseSchema(schema.getSchemaType(), schema.getSchema(), schema.getReferences());
  }

  public ParsedSchema parseSchema(
      String schemaType,
      String schema,
      List<io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference> references)
      throws InvalidSchemaException {
    if (schemaType == null) {
      schemaType = AvroSchema.TYPE;
    }
    SchemaProvider provider = providers.get(schemaType);
    if (provider == null) {
      String errMsg = "Invalid schema type " + schemaType;
      log.error(errMsg);
      throw new InvalidSchemaException(errMsg);
    }
    final String type = schemaType;
    ParsedSchema parsedSchema = provider.parseSchema(schema, references)
            .orElseThrow(() -> new InvalidSchemaException("Invalid schema " + schema
                    + " with refs " + references + " of type " + type));
    return parsedSchema;
  }

  public Schema validateAndGetSchema(String subject, VersionId versionId, boolean
      returnDeletedSchema) throws SchemaRegistryException {
    final int version = versionId.getVersionId();
    Schema schema = this.get(subject, version, returnDeletedSchema);
    if (schema == null) {
      if (!this.hasSubjects(subject, returnDeletedSchema)) {
        throw Errors.subjectNotFoundException(subject);
      } else {
        throw Errors.versionNotFoundException(version);
      }
    }
    return schema;
  }

  public boolean schemaVersionExists(String subject, VersionId versionId, boolean
          returnDeletedSchema) throws SchemaRegistryException {
    final int version = versionId.getVersionId();
    Schema schema = this.get(subject, version, returnDeletedSchema);
    return (schema != null);
  }

  @Override
  public Schema get(String subject, int version, boolean returnDeletedSchema)
      throws SchemaRegistryException {
    VersionId versionId = new VersionId(version);
    if (versionId.isLatest()) {
      return getLatestVersion(subject);
    } else {
      SchemaKey key = new SchemaKey(subject, version);
      try {
        SchemaValue schemaValue = (SchemaValue) kafkaStore.get(key);
        Schema schema = null;
        if ((schemaValue != null && !schemaValue.isDeleted()) || returnDeletedSchema) {
          schema = getSchemaEntityFromSchemaValue(schemaValue);
        }
        return schema;
      } catch (StoreException e) {
        throw new SchemaRegistryStoreException(
            "Error while retrieving schema from the backend Kafka"
            + " store", e);
      }
    }
  }

  @Override
  public SchemaString get(int id) throws SchemaRegistryException {
    return get(id, null, false);
  }

  public SchemaString get(
      int id,
      String format,
      boolean fetchMaxId
  ) throws SchemaRegistryException {
    SchemaValue schema = null;
    try {
      SchemaKey subjectVersionKey = lookupCache.schemaKeyById(id);
      if (subjectVersionKey == null) {
        return null;
      }
      schema = (SchemaValue) kafkaStore.get(subjectVersionKey);
      if (schema == null) {
        return null;
      }
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
          "Error while retrieving schema with id "
          + id
          + " from the backend Kafka"
          + " store", e);
    }
    SchemaString schemaString = new SchemaString();
    schemaString.setSchemaType(schema.getSchemaType());
    List<io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference> refs =
        schema.getReferences() != null
        ? schema.getReferences().stream()
            .map(ref -> new io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference(
                ref.getName(), ref.getSubject(), ref.getVersion()))
          .collect(Collectors.toList())
        : null;
    schemaString.setReferences(refs);
    if (format != null && !format.trim().isEmpty()) {
      ParsedSchema parsedSchema = parseSchema(schema.getSchemaType(), schema.getSchema(), refs);
      schemaString.setSchemaString(parsedSchema.formattedString(format));
    } else {
      schemaString.setSchemaString(schema.getSchema());
    }
    if (fetchMaxId) {
      schemaString.setMaxId(idGenerator.getMaxId(id));
    }
    return schemaString;
  }

  public List<Integer> getReferencedBy(String subject, VersionId versionId)
      throws SchemaRegistryException {
    try {
      int version = versionId.getVersionId();
      if (versionId.isLatest()) {
        version = getLatestVersion(subject).getVersion();
      }
      SchemaKey key = new SchemaKey(subject, version);
      List<Integer> ids = new ArrayList<>(lookupCache.referencesSchema(key));
      Collections.sort(ids);
      return ids;
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
          "Error from the backend Kafka store", e);
    }
  }

  @Override
  public Set<String> listSubjects(boolean returnDeletedSubjects)
          throws SchemaRegistryException {
    try (CloseableIterator<SchemaRegistryKey> allKeys = kafkaStore.getAllKeys()) {
      return extractUniqueSubjects(allKeys,
              returnDeletedSubjects);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
          "Error from the backend Kafka store", e);
    }
  }

  public Set<String> listSubjectsForId(int id) throws SchemaRegistryException {
    return listSubjectsForId(id, false);
  }

  public Set<String> listSubjectsForId(int id, boolean returnDeleted)
      throws SchemaRegistryException {
    List<SubjectVersion> versions = listVersionsForId(id, returnDeleted);
    return versions != null
        ? versions.stream()
            .map(SubjectVersion::getSubject)
            .collect(Collectors.toSet())
        : null;
  }

  public List<SubjectVersion> listVersionsForId(int id) throws SchemaRegistryException {
    return listVersionsForId(id, false);
  }

  public List<SubjectVersion> listVersionsForId(int id, boolean lookupDeleted)
      throws SchemaRegistryException {
    SchemaValue schema = null;
    try {
      SchemaKey subjectVersionKey = lookupCache.schemaKeyById(id);
      if (subjectVersionKey == null) {
        return null;
      }
      schema = (SchemaValue) kafkaStore.get(subjectVersionKey);
      if (schema == null) {
        return null;
      }

      return lookupCache.schemaIdAndSubjects(new Schema(
          schema.getSubject(),
          schema.getVersion(),
          schema.getId(),
          schema.getSchemaType(),
          schema.getReferences().stream()
              .map(ref ->
                  new io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference(
                      ref.getName(), ref.getSubject(), ref.getVersion()))
              .collect(Collectors.toList()),
          schema.getSchema()
      )).allSubjectVersions().entrySet()
          .stream()
          .flatMap(e -> {
            try {
              SchemaValue schemaValue =
                  (SchemaValue) kafkaStore.get(new SchemaKey(e.getKey(), e.getValue()));
              if ((schemaValue != null && !schemaValue.isDeleted()) || lookupDeleted) {
                return Stream.of(new SubjectVersion(e.getKey(), e.getValue()));
              } else {
                return Stream.empty();
              }
            } catch (StoreException ex) {
              return Stream.empty();
            }
          })
          .collect(Collectors.toList());
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Error while retrieving schema with id "
                                              + id + " from the backend Kafka store", e);
    }
  }

  private Set<String> extractUniqueSubjects(Iterator<SchemaRegistryKey> allKeys,
                                            boolean returnDeletedSubjects)
      throws StoreException {
    Set<String> subjects = new HashSet<String>();
    while (allKeys.hasNext()) {
      SchemaRegistryKey k = allKeys.next();
      if (k instanceof SchemaKey) {
        SchemaKey key = (SchemaKey) k;
        SchemaValue value = (SchemaValue) kafkaStore.get(key);
        if (value != null
                && (!value.isDeleted() || returnDeletedSubjects)) {
          subjects.add(key.getSubject());
        }
      }
    }
    return subjects;
  }

  public Set<String> subjects(String subject,
                              boolean lookupDeletedSubjects)
      throws SchemaRegistryStoreException {
    try {
      return lookupCache.subjects(subject, lookupDeletedSubjects);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
          "Error from the backend Kafka store", e);
    }
  }

  public boolean hasSubjects(String subject,
                             boolean lookupDeletedSubjects)
          throws SchemaRegistryStoreException {
    try {
      return lookupCache.hasSubjects(subject, lookupDeletedSubjects);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
          "Error from the backend Kafka store", e);
    }
  }

  @Override
  public Iterator<Schema> getAllVersions(String subject, boolean returnDeletedSchemas)
      throws SchemaRegistryException {
    try (CloseableIterator<SchemaRegistryValue> allVersions = allVersions(subject, false)) {
      return sortSchemasByVersion(allVersions, returnDeletedSchemas).iterator();
    }
  }

  @Override
  public Iterator<Schema> getVersionsWithSubjectPrefix(String prefix,
      boolean returnDeletedSchemas,
      boolean returnLatestOnly)
      throws SchemaRegistryException {
    try (CloseableIterator<SchemaRegistryValue> allVersions = allVersions(prefix, true)) {
      return sortSchemasByVersion(allVersions, returnDeletedSchemas, returnLatestOnly)
          .iterator();
    }
  }

  private List<SchemaValue> getAllSchemaValues(String subject)
      throws SchemaRegistryException {
    try (CloseableIterator<SchemaRegistryValue> allVersions = allVersions(subject, false)) {
      return sortSchemaValuesByVersion(allVersions);
    }
  }

  @Override
  public Schema getLatestVersion(String subject) throws SchemaRegistryException {
    try (CloseableIterator<SchemaRegistryValue> allVersions = allVersions(subject, false)) {
      List<Schema> sortedVersions = sortSchemasByVersion(allVersions, false);
      return sortedVersions.size() > 0 ? sortedVersions.get(sortedVersions.size() - 1) : null;
    }
  }

  private CloseableIterator<SchemaRegistryValue> allVersions(
          String subjectOrPrefix, boolean isPrefix) throws SchemaRegistryException {
    try {
      String start = subjectOrPrefix;
      String end = isPrefix ? subjectOrPrefix + Character.MAX_VALUE : subjectOrPrefix;
      SchemaKey key1 = new SchemaKey(start, MIN_VERSION);
      SchemaKey key2 = new SchemaKey(end, MAX_VERSION);
      return kafkaStore.getAll(key1, key2);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
          "Error from the backend Kafka store", e);
    }
  }

  @Override
  public void close() {
    log.info("Shutting down schema registry");
    kafkaStore.close();
    if (leaderElector != null) {
      leaderElector.close();
    }
  }

  public void updateCompatibilityLevel(String subject, CompatibilityLevel newCompatibilityLevel)
      throws SchemaRegistryStoreException, OperationNotPermittedException, UnknownLeaderException {
    if (getModeInScope(subject) == Mode.READONLY) {
      throw new OperationNotPermittedException("Subject " + subject + " is in read-only mode");
    }
    ConfigKey configKey = new ConfigKey(subject);
    try {
      kafkaStore.waitUntilKafkaReaderReachesLastOffset(subject, kafkaStoreTimeoutMs);
      kafkaStore.put(configKey, new ConfigValue(newCompatibilityLevel));
      log.debug("Wrote new compatibility level: " + newCompatibilityLevel.name + " to the"
                + " Kafka data store with key " + configKey.toString());
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Failed to write new config value to the store",
                                             e);
    }
  }

  public void updateConfigOrForward(String subject, CompatibilityLevel newCompatibilityLevel,
                                    Map<String, String> headerProperties)
      throws SchemaRegistryStoreException, SchemaRegistryRequestForwardingException,
      UnknownLeaderException, OperationNotPermittedException {
    kafkaStore.lockFor(subject).lock();
    try {
      if (isLeader()) {
        updateCompatibilityLevel(subject, newCompatibilityLevel);
      } else {
        // forward update config request to the leader
        if (leaderIdentity != null) {
          forwardUpdateCompatibilityLevelRequestToLeader(subject, newCompatibilityLevel,
                                                         headerProperties);
        } else {
          throw new UnknownLeaderException("Update config request failed since leader is "
                                           + "unknown");
        }
      }
    } finally {
      kafkaStore.lockFor(subject).unlock();
    }
  }

  private String kafkaClusterId(SchemaRegistryConfig config) throws SchemaRegistryException {
    Properties adminClientProps = new Properties();
    KafkaStore.addSchemaRegistryConfigsToClientProperties(config, adminClientProps);
    adminClientProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapBrokers());

    try (AdminClient adminClient = AdminClient.create(adminClientProps)) {
      return adminClient
              .describeCluster()
              .clusterId()
              .get(DESCRIBE_CLUSTER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new SchemaRegistryException("Failed to get Kafka cluster ID", e);
    }
  }

  public String getKafkaClusterId() {
    return kafkaClusterId;
  }

  public CompatibilityLevel getCompatibilityLevel(String subject)
      throws SchemaRegistryStoreException {
    try {
      return lookupCache.compatibilityLevel(subject, false, defaultCompatibilityLevel);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Failed to write new config value to the store", e);
    }
  }

  public CompatibilityLevel getCompatibilityLevelInScope(String subject)
      throws SchemaRegistryStoreException {
    try {
      return lookupCache.compatibilityLevel(subject, true, defaultCompatibilityLevel);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Failed to write new config value to the store", e);
    }
  }

  @Override
  public List<String> isCompatible(String subject,
                                   Schema newSchema,
                                   Schema latestSchema)
      throws SchemaRegistryException {
    if (latestSchema == null) {
      log.error("Lastest schema not provided");
      throw new InvalidSchemaException("Latest schema not provided");
    }
    return isCompatible(subject, newSchema, Collections.singletonList(latestSchema));
  }

  /**
   * @param previousSchemas Full schema history in chronological order
   */
  @Override
  public List<String> isCompatible(String subject,
                                   Schema newSchema,
                                   List<Schema> previousSchemas)
      throws SchemaRegistryException {

    if (previousSchemas == null) {
      log.error("Previous schema not provided");
      throw new InvalidSchemaException("Previous schema not provided");
    }

    List<ParsedSchema> prevParsedSchemas = new ArrayList<>(previousSchemas.size());
    for (Schema previousSchema : previousSchemas) {
      ParsedSchema prevParsedSchema = parseSchema(previousSchema);
      prevParsedSchemas.add(prevParsedSchema);
    }

    ParsedSchema parsedSchema = canonicalizeSchema(newSchema);
    return isCompatibleWithPrevious(subject, parsedSchema, prevParsedSchemas);
  }

  private List<String> isCompatibleWithPrevious(String subject,
                                                ParsedSchema parsedSchema,
                                                List<ParsedSchema> previousSchemas)
      throws SchemaRegistryException {

    CompatibilityLevel compatibility = getCompatibilityLevelInScope(subject);
    return parsedSchema.isCompatible(compatibility, previousSchemas);
  }

  private void deleteMode(String subject) throws StoreException {
    ModeKey modeKey = new ModeKey(subject);
    this.kafkaStore.delete(modeKey);
  }

  private void deleteSubjectCompatibility(String subject) throws StoreException {
    ConfigKey configKey = new ConfigKey(subject);
    this.kafkaStore.delete(configKey);
  }

  public Mode getMode(String subject) throws SchemaRegistryStoreException {
    try {
      return lookupCache.mode(subject, false, defaultMode);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Failed to write new config value to the store", e);
    }
  }

  private Mode getModeInScope(String subject) throws SchemaRegistryStoreException {
    try {
      return lookupCache.mode(subject, true, defaultMode);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Failed to write new config value to the store", e);
    }
  }

  public void setMode(String subject, Mode mode)
      throws SchemaRegistryStoreException, OperationNotPermittedException {
    if (!allowModeChanges) {
      throw new OperationNotPermittedException("Mode changes are not allowed");
    }
    ModeKey modeKey = new ModeKey(subject);
    try {
      kafkaStore.waitUntilKafkaReaderReachesLastOffset(subject, kafkaStoreTimeoutMs);
      if (mode == Mode.IMPORT && getMode(subject) != Mode.IMPORT) {
        // Changing to import mode requires that no schemas exist with matching subjects.
        if (hasSubjects(subject, false)) {
          throw new OperationNotPermittedException("Cannot import since found existing subjects");
        }
        // At this point no schemas should exist with matching subjects.
        // Write an event to clear deleted schemas from the caches.
        kafkaStore.put(new ClearSubjectKey(subject), new ClearSubjectValue(subject));
      }
      kafkaStore.put(modeKey, new ModeValue(mode));
      log.debug("Wrote new mode: " + mode.name() + " to the"
          + " Kafka data store with key " + modeKey.toString());
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Failed to write new mode to the store", e);
    }
  }

  public void setModeOrForward(String subject, Mode mode, Map<String, String> headerProperties)
      throws SchemaRegistryStoreException, SchemaRegistryRequestForwardingException,
      OperationNotPermittedException, UnknownLeaderException {
    kafkaStore.lockFor(subject).lock();
    try {
      if (isLeader()) {
        setMode(subject, mode);
      } else {
        // forward update mode request to the leader
        if (leaderIdentity != null) {
          forwardSetModeRequestToLeader(subject, mode, headerProperties);
        } else {
          throw new UnknownLeaderException("Update mode request failed since leader is "
              + "unknown");
        }
      }
    } finally {
      kafkaStore.lockFor(subject).unlock();
    }
  }

  KafkaStore<SchemaRegistryKey, SchemaRegistryValue> getKafkaStore() {
    return this.kafkaStore;
  }

  private List<Schema> sortSchemasByVersion(CloseableIterator<SchemaRegistryValue> schemas,
                                            boolean returnDeletedSchemas) {
    return sortSchemasByVersion(schemas, returnDeletedSchemas, false);
  }

  private List<Schema> sortSchemasByVersion(CloseableIterator<SchemaRegistryValue> schemas,
                                            boolean returnDeletedSchemas,
                                            boolean returnLatestOnly) {
    List<Schema> schemaList = new ArrayList<>();
    Schema previousSchema = null;
    while (schemas.hasNext()) {
      SchemaValue schemaValue = (SchemaValue) schemas.next();
      if (!returnDeletedSchemas && schemaValue.isDeleted()) {
        continue;
      }
      Schema schema = getSchemaEntityFromSchemaValue(schemaValue);
      if (returnLatestOnly) {
        if (previousSchema != null && !schema.getSubject().equals(previousSchema.getSubject())) {
          schemaList.add(previousSchema);
        }
      } else {
        schemaList.add(schema);
      }
      previousSchema = schema;
    }
    if (returnLatestOnly && previousSchema != null) {
      // handle last subject
      Schema lastSchema = schemaList.isEmpty() ? null : schemaList.get(schemaList.size() - 1);
      if (lastSchema == null || !lastSchema.getSubject().equals(previousSchema.getSubject())) {
        schemaList.add(previousSchema);
      }
    }
    return schemaList;
  }

  private List<SchemaValue> sortSchemaValuesByVersion(
          CloseableIterator<SchemaRegistryValue> schemas) {
    List<SchemaValue> schemaList = new ArrayList<>();
    while (schemas.hasNext()) {
      SchemaValue schemaValue = (SchemaValue) schemas.next();
      schemaList.add(schemaValue);
    }
    return schemaList;
  }

  private Schema getSchemaEntityFromSchemaValue(SchemaValue schemaValue) {
    if (schemaValue == null) {
      return null;
    }
    List<SchemaReference> refs = schemaValue.getReferences();
    return new Schema(
        schemaValue.getSubject(),
        schemaValue.getVersion(),
        schemaValue.getId(),
        schemaValue.getSchemaType(),
        refs == null ? null : refs.stream()
            .map(ref -> new io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference(
                ref.getName(), ref.getSubject(), ref.getVersion()))
            .collect(Collectors.toList()),
        schemaValue.getSchema()
    );
  }

  private boolean isSubjectVersionDeleted(String subject, int version)
      throws SchemaRegistryException {
    try {
      SchemaValue schemaValue = (SchemaValue) this.kafkaStore.get(new SchemaKey(subject, version));
      return schemaValue == null || schemaValue.isDeleted();
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
          "Error while retrieving schema from the backend Kafka"
          + " store", e);
    }
  }

  @Override
  public SchemaRegistryConfig config() {
    return config;
  }

  @Override
  public Map<String, Object> properties() {
    return props;
  }

  public HostnameVerifier getHostnameVerifier() throws SchemaRegistryStoreException {
    String sslEndpointIdentificationAlgo =
            config.getString(RestConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);

    if (sslEndpointIdentificationAlgo == null
            || sslEndpointIdentificationAlgo.equals("none")
            || sslEndpointIdentificationAlgo.isEmpty()) {
      return (hostname, session) -> true;
    }

    if (sslEndpointIdentificationAlgo.equalsIgnoreCase("https")) {
      return null;
    }

    throw new SchemaRegistryStoreException(
            RestConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG
                    + " "
                    + sslEndpointIdentificationAlgo
                    + " not supported");
  }

  public static class SchemeAndPort {
    public int port;
    public String scheme;

    public SchemeAndPort(String scheme, int port) {
      this.port = port;
      this.scheme = scheme;
    }
  }
}
