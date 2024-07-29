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

import static io.confluent.kafka.schemaregistry.client.rest.entities.Metadata.mergeMetadata;
import static io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet.mergeRuleSets;
import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.CONTEXT_DELIMITER;
import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.CONTEXT_PREFIX;
import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.CONTEXT_WILDCARD;
import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.DEFAULT_CONTEXT;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.ParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaTags;
import io.confluent.kafka.schemaregistry.client.rest.entities.ExtendedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.TagSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.rest.utils.UrlList;
import io.confluent.kafka.schemaregistry.client.security.SslFactory;
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
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.leaderelector.kafka.KafkaGroupLeaderElector;
import io.confluent.kafka.schemaregistry.metrics.MetricsContainer;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.VersionId;
import io.confluent.kafka.schemaregistry.rest.extensions.SchemaRegistryResourceExtension;
import io.confluent.kafka.schemaregistry.rest.handlers.CompositeUpdateRequestHandler;
import io.confluent.kafka.schemaregistry.rest.handlers.UpdateRequestHandler;
import io.confluent.kafka.schemaregistry.storage.encoder.MetadataEncoderService;
import io.confluent.kafka.schemaregistry.storage.exceptions.EntryTooLargeException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreTimeoutException;
import io.confluent.kafka.schemaregistry.storage.serialization.Serializer;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.confluent.rest.NamedURI;
import io.confluent.rest.RestConfig;
import io.confluent.rest.exceptions.RestException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.net.ssl.HostnameVerifier;
import org.apache.avro.reflect.Nullable;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSchemaRegistry implements SchemaRegistry, LeaderAwareSchemaRegistry {

  /**
   * Schema versions under a particular subject are indexed from MIN_VERSION.
   */
  public static final int MIN_VERSION = 1;
  public static final int MAX_VERSION = Integer.MAX_VALUE;
  public static final String CONFLUENT_VERSION = "confluent:version";
  private static final Logger log = LoggerFactory.getLogger(KafkaSchemaRegistry.class);
  private static final String RESERVED_FIELD_REMOVED = "The new schema has reserved field %s "
      + "removed from its metadata which is present in the old schema's metadata.";
  private static final String FIELD_CONFLICTS_WITH_RESERVED_FIELD = "The new schema has field that"
      + " conflicts with the reserved field %s.";
  private final SchemaRegistryConfig config;
  private final List<SchemaRegistryResourceExtension> resourceExtensions;
  private final Map<String, Object> props;
  private final LoadingCache<RawSchema, ParsedSchema> schemaCache;
  private final LookupCache<SchemaRegistryKey, SchemaRegistryValue> lookupCache;
  // visible for testing
  final KafkaStore<SchemaRegistryKey, SchemaRegistryValue> kafkaStore;
  private final MetadataEncoderService metadataEncoder;
  private RuleSetHandler ruleSetHandler;
  private final List<UpdateRequestHandler> updateRequestHandlers = new CopyOnWriteArrayList<>();
  private final Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer;
  private final SchemaRegistryIdentity myIdentity;
  private final CompatibilityLevel defaultCompatibilityLevel;
  private final boolean defaultValidateFields;
  private final Mode defaultMode;
  private final int kafkaStoreTimeoutMs;
  private final int initTimeout;
  private final int kafkaStoreMaxRetries;
  private final int searchDefaultLimit;
  private final int searchMaxLimit;
  private final boolean delayLeaderElection;
  private final boolean allowModeChanges;
  private final boolean enableStoreHealthCheck;
  private SchemaRegistryIdentity leaderIdentity;
  private RestService leaderRestService;
  private final SslFactory sslFactory;
  private final int leaderConnectTimeoutMs;
  private final int leaderReadTimeoutMs;
  private final IdGenerator idGenerator;
  private LeaderElector leaderElector = null;
  private final MetricsContainer metricsContainer;
  private final Map<String, SchemaProvider> providers;
  private final String kafkaClusterId;
  private final String groupId;
  private final List<Consumer<Boolean>> leaderChangeListeners = new CopyOnWriteArrayList<>();
  private final AtomicBoolean initialized = new AtomicBoolean(false);
  private final Time time;

  public KafkaSchemaRegistry(SchemaRegistryConfig config,
                             Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer)
      throws SchemaRegistryException {
    if (config == null) {
      throw new SchemaRegistryException("Schema registry configuration is null");
    }
    this.config = config;
    this.resourceExtensions = config.getConfiguredInstances(
        config.definedResourceExtensionConfigName(),
        SchemaRegistryResourceExtension.class);
    this.props = new ConcurrentHashMap<>();
    Boolean leaderEligibility = config.getBoolean(SchemaRegistryConfig.MASTER_ELIGIBILITY);
    if (leaderEligibility == null) {
      leaderEligibility = config.getBoolean(SchemaRegistryConfig.LEADER_ELIGIBILITY);
    }
    this.delayLeaderElection = config.getBoolean(SchemaRegistryConfig.LEADER_ELECTION_DELAY);
    this.allowModeChanges = config.getBoolean(SchemaRegistryConfig.MODE_MUTABILITY);
    this.enableStoreHealthCheck = config.getBoolean(SchemaRegistryConfig.ENABLE_STORE_HEALTH_CHECK);

    String interInstanceListenerNameConfig = config.interInstanceListenerName();
    NamedURI internalListener = getInterInstanceListener(config.getListeners(),
        interInstanceListenerNameConfig,
        config.interInstanceProtocol());
    log.info("Found internal listener: {}", internalListener);
    boolean isEligibleForLeaderElector = leaderEligibility;
    this.myIdentity = getMyIdentity(internalListener, isEligibleForLeaderElector, config);
    log.info("Setting my identity to {}",  myIdentity);

    Map<String, Object> sslConfig = config.getOverriddenSslConfigs(internalListener);
    this.sslFactory =
        new SslFactory(ConfigDef.convertToStringMapWithPasswordValues(sslConfig));
    this.leaderConnectTimeoutMs = config.getInt(SchemaRegistryConfig.LEADER_CONNECT_TIMEOUT_MS);
    this.leaderReadTimeoutMs = config.getInt(SchemaRegistryConfig.LEADER_READ_TIMEOUT_MS);
    this.kafkaStoreTimeoutMs =
        config.getInt(SchemaRegistryConfig.KAFKASTORE_TIMEOUT_CONFIG);
    this.initTimeout = config.getInt(SchemaRegistryConfig.KAFKASTORE_INIT_TIMEOUT_CONFIG);
    this.kafkaStoreMaxRetries =
        config.getInt(SchemaRegistryConfig.KAFKASTORE_WRITE_MAX_RETRIES_CONFIG);
    this.serializer = serializer;
    this.defaultCompatibilityLevel = config.compatibilityType();
    this.defaultValidateFields =
        config.getBoolean(SchemaRegistryConfig.SCHEMA_VALIDATE_FIELDS_CONFIG);
    this.defaultMode = Mode.READWRITE;
    this.kafkaClusterId = kafkaClusterId(config);
    this.groupId = config.getString(SchemaRegistryConfig.SCHEMAREGISTRY_GROUP_ID_CONFIG);
    this.metricsContainer = new MetricsContainer(config, this.kafkaClusterId);
    this.providers = initProviders(config);
    this.schemaCache = Caffeine.newBuilder()
        .maximumSize(config.getInt(SchemaRegistryConfig.SCHEMA_CACHE_SIZE_CONFIG))
        .expireAfterAccess(config.getInt(SchemaRegistryConfig.SCHEMA_CACHE_EXPIRY_SECS_CONFIG),
                TimeUnit.SECONDS)
        .build(s -> loadSchema(s.getSchema(), s.isNew(), s.isNormalize()));
    this.searchDefaultLimit =
        config.getInt(SchemaRegistryConfig.SCHEMA_SEARCH_DEFAULT_LIMIT_CONFIG);
    this.searchMaxLimit = config.getInt(SchemaRegistryConfig.SCHEMA_SEARCH_MAX_LIMIT_CONFIG);
    this.lookupCache = lookupCache();
    this.idGenerator = identityGenerator(config);
    this.kafkaStore = kafkaStore(config);
    this.metadataEncoder = new MetadataEncoderService(this);
    this.ruleSetHandler = new RuleSetHandler();
    this.time = config.getTime();
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
    metricsContainer.getCustomSchemaProviderCount().record(customSchemaProviders.size());
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

  public List<SchemaRegistryResourceExtension> getResourceExtensions() {
    return resourceExtensions;
  }

  protected LookupCache<SchemaRegistryKey, SchemaRegistryValue> lookupCache() {
    return new InMemoryCache<>(serializer);
  }

  public LookupCache<SchemaRegistryKey, SchemaRegistryValue> getLookupCache() {
    return lookupCache;
  }

  public Serializer<SchemaRegistryKey, SchemaRegistryValue> getSerializer() {
    return serializer;
  }

  public MetadataEncoderService getMetadataEncoder() {
    return metadataEncoder;
  }

  public RuleSetHandler getRuleSetHandler() {
    return ruleSetHandler;
  }

  public void setRuleSetHandler(RuleSetHandler ruleSetHandler) {
    this.ruleSetHandler = ruleSetHandler;
  }

  public UpdateRequestHandler getCompositeUpdateRequestHandler() {
    List<UpdateRequestHandler> handlers = new ArrayList<>();
    handlers.add(ruleSetHandler);
    handlers.addAll(updateRequestHandlers);
    return new CompositeUpdateRequestHandler(handlers);
  }

  public void addUpdateRequestHandler(UpdateRequestHandler updateRequestHandler) {
    updateRequestHandlers.add(updateRequestHandler);
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

  public MetricsContainer getMetricsContainer() {
    return metricsContainer;
  }

  /**
   * <p>This method returns a listener to be used for inter-instance communication.
   * It iterates through the list of listeners until it finds one whose name
   * matches the inter.instance.listener.name config. If no such listener is found,
   * it returns the last listener matching the requested scheme.
   * </p>
   * <p>When there is no matching named listener, in theory, any port from any listener
   * would be sufficient. Choosing the last, instead of say the first, is arbitrary.
   * The port used by this listener also forms the identity of the schema registry instance
   * along with the host name.
   * </p>
   */
  // TODO: once RestConfig.PORT_CONFIG is deprecated, remove the port parameter.
  public static NamedURI getInterInstanceListener(List<NamedURI> listeners,
                                             String interInstanceListenerName,
                                             String requestedScheme)
      throws SchemaRegistryException {
    if (requestedScheme.isEmpty()) {
      requestedScheme = SchemaRegistryConfig.HTTP;
    }

    NamedURI internalListener = null;
    for (NamedURI listener : listeners) {
      if (listener.getName() !=  null
              && listener.getName().equalsIgnoreCase(interInstanceListenerName)) {
        internalListener = listener;
        break;
      } else if (listener.getUri().getScheme().equalsIgnoreCase(requestedScheme)) {
        internalListener = listener;
      }
    }
    if (internalListener == null) {
      throw new SchemaRegistryException(" No listener configured with requested scheme "
                                          + requestedScheme);
    }
    return internalListener;
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

  public SslFactory getSslFactory() {
    return sslFactory;
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
        leaderRestService = new RestService(leaderIdentity.getUrl(),
            config.whitelistHeaders().contains(RestService.X_FORWARD_HEADER));
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
          //ensure the new leader catches up with the offsets before it gets nextid and assigns
          // leader
          try {
            kafkaStore.waitUntilKafkaReaderReachesLastOffset(initTimeout);
          } catch (StoreException e) {
            throw new SchemaRegistryStoreException("Exception getting latest offset ", e);
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

  public Set<String> schemaTypes() {
    return providers.keySet();
  }

  public SchemaProvider schemaProvider(String schemaType) {
    return providers.get(schemaType);
  }

  public int normalizeLimit(int suppliedLimit) {
    int limit = searchDefaultLimit;
    if (suppliedLimit > 0 && suppliedLimit <= searchMaxLimit) {
      limit = suppliedLimit;
    }
    return limit;
  }

  public Schema register(String subject, RegisterSchemaRequest request, boolean normalize)
      throws SchemaRegistryException {
    try {
      Schema schema = new Schema(subject, request);

      if (request.hasSchemaTagsToAddOrRemove()) {
        ParsedSchema parsedSchema = parseSchema(schema);
        ParsedSchema newSchema = parsedSchema
            .copy(TagSchemaRequest.schemaTagsListToMap(request.getSchemaTagsToAdd()),
                TagSchemaRequest.schemaTagsListToMap(request.getSchemaTagsToRemove()));
        // If a version was not specified, then use the latest version
        // to ensure that the confluent:version metadata is added
        int version = request.getVersion() != null ? request.getVersion() : -1;
        schema = new Schema(subject, version, schema.getId(), newSchema);
      }

      return register(subject, schema, normalize);
    } catch (IllegalArgumentException e) {
      throw new InvalidSchemaException(e);
    }
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
                         boolean normalize)
      throws SchemaRegistryException {
    try {
      checkRegisterMode(subject, schema);

      // Ensure cache is up-to-date before any potential writes
      kafkaStore.waitUntilKafkaReaderReachesLastOffset(subject, kafkaStoreTimeoutMs);

      // determine the latest version of the schema in the subject
      List<SchemaKey> allVersions = getAllSchemaKeys(subject);
      // sort versions in descending
      Collections.reverse(allVersions);

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

      boolean modifiedSchema = false;
      if (mode != Mode.IMPORT) {
        modifiedSchema = maybePopulateFromPrevious(config, schema, undeletedVersions, newVersion);
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
            return modifiedSchema
                ? schema.copy(
                    schemaIdAndSubjects.getVersion(subject), schemaIdAndSubjects.getSchemaId())
                : new Schema(subject, schemaIdAndSubjects.getSchemaId());
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
              && parsedSchema.references().isEmpty()
              && !undeletedSchema.references().isEmpty()
              && parsedSchema.deepEquals(undeletedSchema)
              && (schemaId < 0 || schemaId == schemaValue.getId())) {
            // This handles the case where a schema is sent with all references resolved
            return modifiedSchema
                ? schema.copy(schemaValue.getVersion(), schemaValue.getId())
                : new Schema(subject, schemaValue.getId());
          }
        }
      }

      boolean isCompatible = true;
      List<String> compatibilityErrorLogs = new ArrayList<>();
      if (mode != Mode.IMPORT) {
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
        } else if (newVersion != schema.getVersion() && mode != Mode.IMPORT) {
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
                log.warn(String.format("Retrying to register the schema with ID %s", newId));
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

        return modifiedSchema
            ? schema
            : new Schema(subject, schema.getId());
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

  private void checkRegisterMode(
      String subject, Schema schema
  ) throws OperationNotPermittedException, SchemaRegistryStoreException {
    if (isReadOnlyMode(subject)) {
      throw new OperationNotPermittedException("Subject " + subject + " is in read-only mode");
    }

    if (schema.getId() >= 0) {
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

  private boolean isReadOnlyMode(String subject) throws SchemaRegistryStoreException {
    Mode subjectMode = getModeInScope(subject);
    return subjectMode == Mode.READONLY || subjectMode == Mode.READONLY_OVERRIDE;
  }

  private boolean maybePopulateFromPrevious(
      Config config, Schema schema, List<ParsedSchemaHolder> undeletedVersions, int newVersion)
      throws SchemaRegistryException {
    boolean populatedSchema = false;
    SchemaValue previousSchemaValue = !undeletedVersions.isEmpty()
        ? ((LazyParsedSchemaHolder) undeletedVersions.get(0)).schemaValue()
        : null;
    Schema previousSchema = previousSchemaValue != null
        ? toSchemaEntity(previousSchemaValue)
        : null;
    if (schema == null
        || schema.getSchema() == null
        || schema.getSchema().trim().isEmpty()) {
      if (previousSchemaValue != null) {
        schema.setSchema(previousSchema.getSchema());
        schema.setSchemaType(previousSchema.getSchemaType());
        schema.setReferences(previousSchema.getReferences());
        populatedSchema = true;
      } else {
        throw new InvalidSchemaException("Empty schema");
      }
    }
    boolean populatedMetadataRuleSet = maybeSetMetadataRuleSet(
        config, schema, previousSchema, newVersion);
    return populatedSchema || populatedMetadataRuleSet;
  }

  private boolean maybeSetMetadataRuleSet(
      Config config, Schema schema, Schema previousSchema, int newVersion) {
    io.confluent.kafka.schemaregistry.client.rest.entities.Metadata specificMetadata = null;
    if (schema.getMetadata() != null) {
      specificMetadata = schema.getMetadata();
    } else if (previousSchema != null) {
      specificMetadata = previousSchema.getMetadata();
    }
    io.confluent.kafka.schemaregistry.client.rest.entities.Metadata mergedMetadata;
    io.confluent.kafka.schemaregistry.client.rest.entities.Metadata defaultMetadata;
    io.confluent.kafka.schemaregistry.client.rest.entities.Metadata overrideMetadata;
    defaultMetadata = config.getDefaultMetadata();
    overrideMetadata = config.getOverrideMetadata();
    mergedMetadata =
        mergeMetadata(mergeMetadata(defaultMetadata, specificMetadata), overrideMetadata);
    io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet specificRuleSet = null;
    if (schema.getRuleSet() != null) {
      specificRuleSet = schema.getRuleSet();
    } else if (previousSchema != null) {
      specificRuleSet = previousSchema.getRuleSet();
    }
    io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet mergedRuleSet;
    io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet defaultRuleSet;
    io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet overrideRuleSet;
    defaultRuleSet = config.getDefaultRuleSet();
    overrideRuleSet = config.getOverrideRuleSet();
    mergedRuleSet = mergeRuleSets(mergeRuleSets(defaultRuleSet, specificRuleSet), overrideRuleSet);

    // Set confluent:version if passed in version is not 0,
    // or update confluent:version if it already exists in the metadata
    if (schema.getVersion() != 0
        || (mergedMetadata != null
        && mergedMetadata.getProperties() != null
        && mergedMetadata.getProperties().containsKey(CONFLUENT_VERSION))) {
      Map<String, String> newProps =
          Collections.singletonMap(CONFLUENT_VERSION, String.valueOf(newVersion));
      mergedMetadata = mergeMetadata(mergedMetadata,
          new io.confluent.kafka.schemaregistry.client.rest.entities.Metadata(
              null, newProps, null));
    }

    if (mergedMetadata != null || mergedRuleSet != null) {
      schema.setMetadata(mergedMetadata);
      schema.setRuleSet(mergedRuleSet);
      return true;
    }
    return false;
  }

  public Schema registerOrForward(String subject,
                                  RegisterSchemaRequest request,
                                  boolean normalize,
                                  Map<String, String> headerProperties)
      throws SchemaRegistryException {
    Schema schema = new Schema(subject, request);
    Config config = getConfigInScope(subject);
    if (!request.hasSchemaTagsToAddOrRemove()
        && schema.getVersion() != -1
        && !config.hasDefaultsOrOverrides()) {
      Schema existingSchema = lookUpSchemaUnderSubject(subject, schema, normalize, false);
      if (existingSchema != null) {
        if (schema.getVersion() == 0) {
          if (schema.getId() == null
              || schema.getId() < 0
              || schema.getId().equals(existingSchema.getId())
          ) {
            return new Schema(subject, existingSchema.getId());
          }
        } else if (existingSchema.getId().equals(schema.getId())
            && existingSchema.getVersion().equals(schema.getVersion())) {
          return existingSchema;
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
          return forwardRegisterRequestToLeader(subject, schema, normalize, headerProperties);
        } else {
          throw new UnknownLeaderException("Register schema request failed since leader is "
                                           + "unknown");
        }
      }
    } finally {
      kafkaStore.lockFor(subject).unlock();
    }
  }

  public void extractSchemaTags(Schema schema, List<String> tags)
      throws SchemaRegistryException {
    ParsedSchema parsedSchema = parseSchema(schema);
    boolean isWildcard = tags.contains("*");
    List<SchemaTags> schemaTags = parsedSchema.inlineTaggedEntities().entrySet()
        .stream()
        .filter(e -> isWildcard || !Collections.disjoint(tags, e.getValue()))
        .map(e -> new SchemaTags(e.getKey(), new ArrayList<>(e.getValue())))
        .collect(Collectors.toList());
    schema.setSchemaTags(schemaTags);
  }

  public Schema modifySchemaTags(String subject, Schema schema, TagSchemaRequest request)
      throws SchemaRegistryException {
    ParsedSchema parsedSchema = parseSchema(schema);
    int newVersion = request.getNewVersion() != null ? request.getNewVersion() : 0;

    Metadata mergedMetadata = request.getMetadata() != null
        ? request.getMetadata()
        : parsedSchema.metadata();
    Metadata newMetadata = new Metadata(
        Collections.emptyMap(),
        Collections.singletonMap(CONFLUENT_VERSION, String.valueOf(newVersion)),
        Collections.emptySet());
    mergedMetadata = Metadata.mergeMetadata(mergedMetadata, newMetadata);

    RuleSet ruleSet = maybeModifyPreviousRuleSet(subject, request);

    try {
      ParsedSchema newSchema = parsedSchema
          .copy(TagSchemaRequest.schemaTagsListToMap(request.getTagsToAdd()),
              TagSchemaRequest.schemaTagsListToMap(request.getTagsToRemove()))
          .copy(mergedMetadata, ruleSet)
          .copy(newVersion);
      return register(subject, new Schema(subject, newVersion, -1, newSchema), false);
    } catch (IllegalArgumentException e) {
      throw new InvalidSchemaException(e);
    }
  }

  private RuleSet maybeModifyPreviousRuleSet(String subject, TagSchemaRequest request)
      throws SchemaRegistryException {
    if (request.getRulesToMerge() == null && request.getRulesToRemove() == null) {
      return request.getRuleSet();
    }
    int oldVersion = request.getNewVersion() != null ? request.getNewVersion() - 1 : -1;
    Schema oldSchema = get(subject, oldVersion, false);
    // Use the previous ruleSet instead of the passed in one
    RuleSet ruleSet = oldSchema != null ? oldSchema.getRuleSet() : null;
    if (request.getRulesToMerge() != null) {
      ruleSet = mergeRuleSets(ruleSet, request.getRulesToMerge());
    }
    if (ruleSet != null && request.getRulesToRemove() != null) {
      List<String> rulesToRemove = request.getRulesToRemove();
      List<Rule> migrationRules = ruleSet.getMigrationRules();
      if (migrationRules != null) {
        migrationRules = migrationRules.stream()
            .filter(r -> !rulesToRemove.contains(r.getName()))
            .collect(Collectors.toList());
      }
      List<Rule> domainRules = ruleSet.getDomainRules();
      if (domainRules != null) {
        domainRules = domainRules.stream()
            .filter(r -> !rulesToRemove.contains(r.getName()))
            .collect(Collectors.toList());
      }
      ruleSet = new RuleSet(migrationRules, domainRules);
    }
    return ruleSet;
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
        throw new OperationNotPermittedException("Subject " + subject + " is in read-only mode");
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
        if (!getAllVersions(subject, LookupFilter.DEFAULT).hasNext()) {
          if (getMode(subject) != null) {
            deleteMode(subject);
          }
          if (getConfig(subject) != null) {
            deleteConfig(subject);
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
      if (isReadOnlyMode(subject)) {
        throw new OperationNotPermittedException("Subject " + subject + " is in read-only mode");
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
        if (getMode(subject) != null) {
          deleteMode(subject);
        }
        if (getConfig(subject) != null) {
          deleteConfig(subject);
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

  public Schema lookUpSchemaUnderSubjectUsingContexts(
      String subject, Schema schema, boolean normalize, boolean lookupDeletedSchema)
      throws SchemaRegistryException {
    Schema matchingSchema =
        lookUpSchemaUnderSubject(subject, schema, normalize, lookupDeletedSchema);
    if (matchingSchema != null) {
      return matchingSchema;
    }
    QualifiedSubject qs = QualifiedSubject.create(tenant(), subject);
    boolean isQualifiedSubject = qs != null && !DEFAULT_CONTEXT.equals(qs.getContext());
    if (isQualifiedSubject) {
      return null;
    }
    // Try qualifying the subject with each known context
    try (CloseableIterator<SchemaRegistryValue> iter = allContexts()) {
      while (iter.hasNext()) {
        ContextValue v = (ContextValue) iter.next();
        QualifiedSubject qualSub =
            new QualifiedSubject(v.getTenant(), v.getContext(), qs.getSubject());
        Schema qualSchema = schema.copy();
        qualSchema.setSubject(qualSub.toQualifiedSubject());
        matchingSchema = lookUpSchemaUnderSubject(
            qualSub.toQualifiedSubject(), qualSchema, normalize, lookupDeletedSchema);
        if (matchingSchema != null) {
          return matchingSchema;
        }
      }
    }
    return null;
  }

  /**
   * Checks if given schema was ever registered under a subject. If found, it returns the version of
   * the schema under the subject. If not, returns -1
   */
  public Schema lookUpSchemaUnderSubject(
      String subject, Schema schema, boolean normalize, boolean lookupDeletedSchema)
      throws SchemaRegistryException {
    try {
      // Pass a copy of the schema so the original is not modified during normalization
      // to ensure that invalid defaults are not dropped since default validation is disabled
      Schema newSchema = schema != null ? schema.copy() : null;
      Config config = getConfigInScope(subject);
      ParsedSchema parsedSchema = canonicalizeSchema(newSchema, config, false, normalize);
      if (parsedSchema != null) {
        SchemaIdAndSubjects schemaIdAndSubjects = this.lookupCache.schemaIdAndSubjects(newSchema);
        if (schemaIdAndSubjects != null) {
          if (schemaIdAndSubjects.hasSubject(subject)
              && (lookupDeletedSchema || !isSubjectVersionDeleted(subject, schemaIdAndSubjects
              .getVersion(subject)))) {
            Schema matchingSchema = newSchema.copy();
            matchingSchema.setSubject(subject);
            matchingSchema.setVersion(schemaIdAndSubjects.getVersion(subject));
            matchingSchema.setId(schemaIdAndSubjects.getSchemaId());
            return matchingSchema;
          }
        }
      }

      List<SchemaKey> allVersions = getAllSchemaKeys(subject);
      Collections.reverse(allVersions);

      for (SchemaKey schemaKey : allVersions) {
        Schema prev = get(schemaKey.getSubject(), schemaKey.getVersion(), lookupDeletedSchema);
        if (prev != null
            && parsedSchema != null
            && parsedSchema.references().isEmpty()
            && !prev.getReferences().isEmpty()) {
          ParsedSchema prevSchema = parseSchema(prev);
          if (parsedSchema.deepEquals(prevSchema)) {
            // This handles the case where a schema is sent with all references resolved
            return prev;
          }
        }
      }

      return null;
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
          "Error from the backend Kafka store", e);
    }
  }

  public Schema getLatestWithMetadata(
      String subject, Map<String, String> metadata, boolean lookupDeletedSchema)
      throws SchemaRegistryException {
    List<SchemaKey> allVersions = getAllSchemaKeys(subject);
    Collections.reverse(allVersions);

    for (SchemaKey schemaKey : allVersions) {
      Schema schema = get(schemaKey.getSubject(), schemaKey.getVersion(), lookupDeletedSchema);
      if (schema != null) {
        if (schema.getMetadata() != null) {
          Map<String, String> props = schema.getMetadata().getProperties();
          if (props != null && props.entrySet().containsAll(metadata.entrySet())) {
            return schema;
          }
        }
      }
    }

    return null;
  }

  public void checkIfSchemaWithIdExist(int id, Schema schema)
      throws SchemaRegistryException, StoreException {
    String qctx = QualifiedSubject.qualifiedContextFor(tenant(), schema.getSubject());
    SchemaKey existingKey = this.lookupCache.schemaKeyById(id, qctx);
    if (existingKey != null) {
      SchemaRegistryValue existingValue = this.lookupCache.get(existingKey);
      if (existingValue instanceof SchemaValue) {
        SchemaValue existingSchemaValue = (SchemaValue) existingValue;
        Schema existingSchema = toSchemaEntity(existingSchemaValue);
        Schema schemaCopy = schema.copy();
        schemaCopy.setId(existingSchema.getId());
        schemaCopy.setSubject(existingSchema.getSubject());
        schemaCopy.setVersion(existingSchema.getVersion());
        if (!existingSchema.equals(schemaCopy)) {
          throw new OperationNotPermittedException(
              String.format("Overwrite new schema with id %s is not permitted.", id)
          );
        }
      }
    }
  }

  private Schema forwardRegisterRequestToLeader(
      String subject, Schema schema, boolean normalize, Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    final UrlList baseUrl = leaderRestService.getBaseUrls();

    RegisterSchemaRequest registerSchemaRequest = new RegisterSchemaRequest(schema);
    log.debug(String.format("Forwarding registering schema request to %s", baseUrl));
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

    log.debug(String.format("Forwarding register schema tags request to %s", baseUrl));
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

  private void forwardUpdateConfigRequestToLeader(
      String subject, Config config,
      Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    UrlList baseUrl = leaderRestService.getBaseUrls();

    ConfigUpdateRequest configUpdateRequest = new ConfigUpdateRequest(config);
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

  private void forwardDeleteConfigToLeader(
      Map<String, String> requestProperties,
      String subject
  ) throws SchemaRegistryRequestForwardingException {
    UrlList baseUrl = leaderRestService.getBaseUrls();

    log.debug(String.format("Forwarding delete subject compatibility config request %s to %s",
        subject, baseUrl));
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
      String subject, Mode mode, boolean force,
      Map<String, String> headerProperties)
      throws SchemaRegistryRequestForwardingException {
    UrlList baseUrl = leaderRestService.getBaseUrls();

    ModeUpdateRequest modeUpdateRequest = new ModeUpdateRequest();
    modeUpdateRequest.setMode(mode.name());
    log.debug(String.format("Forwarding update mode request %s to %s",
        modeUpdateRequest, baseUrl));
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

    log.debug(String.format("Forwarding delete subject mode request %s to %s",
        subject, baseUrl));
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

  private ParsedSchema canonicalizeSchema(Schema schema,
                                          Config config,
                                          boolean isNew,
                                          boolean normalize) throws InvalidSchemaException {
    if (schema == null
        || schema.getSchema() == null
        || schema.getSchema().trim().isEmpty()) {
      return null;
    }
    ParsedSchema parsedSchema = parseSchema(schema, isNew, normalize);
    return maybeValidateAndNormalizeSchema(parsedSchema, schema, config, normalize);
  }

  private ParsedSchema maybeValidateAndNormalizeSchema(ParsedSchema parsedSchema,
                                                       Schema schema,
                                                       Config config,
                                                       boolean normalize)
          throws InvalidSchemaException {
    try {
      if (getModeInScope(schema.getSubject()) != Mode.IMPORT) {
        parsedSchema.validate(isSchemaFieldValidationEnabled(config));
      }
      if (normalize) {
        parsedSchema = parsedSchema.normalize();
      }
    } catch (Exception e) {
      String errMsg = "Invalid schema " + schema + ", details: " + e.getMessage();
      log.error(errMsg, e);
      throw new InvalidSchemaException(errMsg, e);
    }
    schema.setSchemaType(parsedSchema.schemaType());
    schema.setSchema(parsedSchema.canonicalString());
    schema.setReferences(parsedSchema.references());
    return parsedSchema;
  }

  public ParsedSchema parseSchema(Schema schema) throws InvalidSchemaException {
    return parseSchema(schema, false, false);
  }

  public ParsedSchema parseSchema(
          Schema schema,
          boolean isNew,
          boolean normalize) throws InvalidSchemaException {
    try {
      ParsedSchema parsedSchema = schemaCache.get(new RawSchema(schema.copy(), isNew, normalize));
      if (schema.getVersion() != null) {
        parsedSchema = parsedSchema.copy(schema.getVersion());
      }
      return parsedSchema;
    } catch (Exception e) {
      Throwable cause = e.getCause();
      if (cause instanceof InvalidSchemaException) {
        throw (InvalidSchemaException) cause;
      } else if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  private ParsedSchema loadSchema(
      Schema schema,
      boolean isNew,
      boolean normalize)
      throws InvalidSchemaException {
    String schemaType = schema.getSchemaType();
    if (schemaType == null) {
      schemaType = AvroSchema.TYPE;
    }
    SchemaProvider provider = schemaProvider(schemaType);
    if (provider == null) {
      String errMsg = "Invalid schema type " + schemaType;
      log.error(errMsg);
      throw new InvalidSchemaException(errMsg);
    }
    final String type = schemaType;

    try {
      return provider.parseSchemaOrElseThrow(schema, isNew, normalize);
    } catch (Exception e) {
      throw new InvalidSchemaException("Invalid schema " + schema
              + " with refs " + schema.getReferences()
              + " of type " + type + ", details: " + e.getMessage());
    }
  }

  public Schema getUsingContexts(String subject, int version, boolean
      returnDeletedSchema) throws SchemaRegistryException {
    Schema schema = get(subject, version, returnDeletedSchema);
    if (schema != null) {
      return schema;
    }
    QualifiedSubject qs = QualifiedSubject.create(tenant(), subject);
    boolean isQualifiedSubject = qs != null && !DEFAULT_CONTEXT.equals(qs.getContext());
    if (isQualifiedSubject) {
      return null;
    }
    // Try qualifying the subject with each known context
    try (CloseableIterator<SchemaRegistryValue> iter = allContexts()) {
      while (iter.hasNext()) {
        ContextValue v = (ContextValue) iter.next();
        QualifiedSubject qualSub =
            new QualifiedSubject(v.getTenant(), v.getContext(), qs.getSubject());
        schema = get(qualSub.toQualifiedSubject(), version, returnDeletedSchema);
        if (schema != null) {
          return schema;
        }
      }
    }
    return null;
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
      SchemaValue schemaValue = getSchemaValue(new SchemaKey(subject, version));
      Schema schema = null;
      if (schemaValue != null && (!schemaValue.isDeleted() || returnDeletedSchema)) {
        schema = toSchemaEntity(schemaValue);
      }
      return schema;
    }
  }

  @Override
  public SchemaString get(int id, String subject) throws SchemaRegistryException {
    return get(id, subject, null, false);
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
    SchemaString schemaString = new SchemaString(schemaEntity);
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

  public Schema toSchemaEntity(SchemaValue schemaValue) {
    metadataEncoder.decodeMetadata(schemaValue);
    return schemaValue.toSchemaEntity();
  }

  protected SchemaValue getSchemaValue(SchemaKey key)
      throws SchemaRegistryException {
    try {
      return (SchemaValue) kafkaStore.get(key);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
          "Error while retrieving schema from the backend Kafka"
              + " store", e);
    }
  }

  private SchemaKey getSchemaKeyUsingContexts(int id, String subject)
          throws StoreException, SchemaRegistryException {
    QualifiedSubject qs = QualifiedSubject.create(tenant(), subject);
    boolean isQualifiedSubject = qs != null && !DEFAULT_CONTEXT.equals(qs.getContext());
    SchemaKey subjectVersionKey = lookupCache.schemaKeyById(id, subject);
    if (qs == null
        || qs.getSubject().isEmpty()
        || isQualifiedSubject
        || subjectVersionKey != null) {
      return subjectVersionKey;
    }
    // Try qualifying the subject with each known context
    try (CloseableIterator<SchemaRegistryValue> iter = allContexts()) {
      while (iter.hasNext()) {
        ContextValue v = (ContextValue) iter.next();
        QualifiedSubject qualSub =
            new QualifiedSubject(v.getTenant(), v.getContext(), qs.getSubject());
        SchemaKey key = lookupCache.schemaKeyById(id, qualSub.toQualifiedSubject());
        if (key != null) {
          return key;
        }
      }
    }
    // Could not find the id in subjects in other contexts,
    // just return the id in the given context if found
    return lookupCache.schemaKeyById(id, qs.toQualifiedContext());
  }

  private CloseableIterator<SchemaRegistryValue> allContexts() throws SchemaRegistryException {
    try {
      ContextKey key1 = new ContextKey(tenant(), String.valueOf(Character.MIN_VALUE));
      ContextKey key2 = new ContextKey(tenant(), String.valueOf(Character.MAX_VALUE));
      return kafkaStore.getAll(key1, key2);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
              "Error from the backend Kafka store", e);
    }
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

  public List<String> listContexts() throws SchemaRegistryException {
    List<String> contexts = new ArrayList<>();
    contexts.add(DEFAULT_CONTEXT);
    try (CloseableIterator<SchemaRegistryValue> iter = allContexts()) {
      while (iter.hasNext()) {
        ContextValue contextValue = (ContextValue) iter.next();
        contexts.add(contextValue.getContext());
      }
    }
    return contexts;
  }

  @Override
  public Set<String> listSubjects(LookupFilter filter)
          throws SchemaRegistryException {
    return listSubjectsWithPrefix(CONTEXT_WILDCARD, filter);
  }

  public Set<String> listSubjectsWithPrefix(String prefix, LookupFilter filter)
      throws SchemaRegistryException {
    try (CloseableIterator<SchemaRegistryValue> allVersions = allVersions(prefix, true)) {
      return extractUniqueSubjects(allVersions, filter);
    }
  }

  public Set<String> listSubjectsForId(int id, String subject) throws SchemaRegistryException {
    return listSubjectsForId(id, subject, false);
  }

  @Override
  public Set<String> listSubjectsForId(int id, String subject, boolean returnDeleted)
      throws SchemaRegistryException {
    List<SubjectVersion> versions = listVersionsForId(id, subject, returnDeleted);
    return versions != null
        ? versions.stream()
            .map(SubjectVersion::getSubject)
            .collect(Collectors.toCollection(LinkedHashSet::new))
        : null;
  }

  public List<SubjectVersion> listVersionsForId(int id, String subject)
      throws SchemaRegistryException {
    return listVersionsForId(id, subject, false);
  }

  public List<SubjectVersion> listVersionsForId(int id, String subject, boolean lookupDeleted)
      throws SchemaRegistryException {
    SchemaValue schema;
    try {
      SchemaKey subjectVersionKey = getSchemaKeyUsingContexts(id, subject);
      if (subjectVersionKey == null) {
        return null;
      }
      schema = (SchemaValue) kafkaStore.get(subjectVersionKey);
      if (schema == null) {
        return null;
      }

      return lookupCache.schemaIdAndSubjects(toSchemaEntity(schema))
          .allSubjectVersions()
          .entrySet()
          .stream()
          .flatMap(e -> {
            try {
              SchemaValue schemaValue =
                  (SchemaValue) kafkaStore.get(new SchemaKey(e.getKey(), e.getValue()));
              if (schemaValue != null && (!schemaValue.isDeleted() || lookupDeleted)) {
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

  private Set<String> extractUniqueSubjects(Iterator<SchemaRegistryValue> allVersions,
                                            LookupFilter filter) {
    Map<String, Boolean> subjects = new HashMap<>();
    while (allVersions.hasNext()) {
      SchemaValue value = (SchemaValue) allVersions.next();
      subjects.merge(value.getSubject(), value.isDeleted(), (v1, v2) -> v1 && v2);
    }

    return subjects.keySet().stream()
        .filter(k -> shouldInclude(subjects.get(k), filter))
        .sorted()
        .collect(Collectors.toCollection(LinkedHashSet::new));
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
  public Iterator<SchemaKey> getAllVersions(String subject, LookupFilter filter)
      throws SchemaRegistryException {
    try (CloseableIterator<SchemaRegistryValue> allVersions = allVersions(subject, false)) {
      List<SchemaKey> schemaKeys = schemaKeysByVersion(allVersions, filter);
      Collections.sort(schemaKeys);
      return schemaKeys.iterator();
    }
  }

  @Override
  public Iterator<ExtendedSchema> getVersionsWithSubjectPrefix(String prefix,
      boolean includeAliases,
      LookupFilter filter,
      boolean returnLatestOnly,
      Predicate<Schema> postFilter)
      throws SchemaRegistryException {
    if (includeAliases) {
      return allVersionsIncludingAliasesWithSubjectPrefix(
          prefix, filter, returnLatestOnly, postFilter);
    } else {
      return allVersionsWithSubjectPrefix(prefix, filter, returnLatestOnly, postFilter);
    }
  }

  private Iterator<ExtendedSchema> allVersionsWithSubjectPrefix(String prefix,
      LookupFilter filter,
      boolean returnLatestOnly,
      Predicate<Schema> postFilter)
      throws SchemaRegistryException {
    try (CloseableIterator<SchemaRegistryValue> allVersions = allVersions(prefix, true)) {
      Map<String, List<ExtendedSchema>> schemas = schemasByVersion(
          Collections.emptyMap(), allVersions, filter, returnLatestOnly, postFilter);
      List<ExtendedSchema> result = new ArrayList<>();
      for (List<ExtendedSchema> schemaList : schemas.values()) {
        result.addAll(schemaList);
      }
      Collections.sort(result);
      return result.iterator();
    }
  }

  public Iterator<ExtendedSchema> allVersionsIncludingAliasesWithSubjectPrefix(String prefix,
      LookupFilter filter,
      boolean returnLatestOnly,
      Predicate<Schema> postFilter)
      throws SchemaRegistryException {
    Map<String, List<String>> aliases = getAliases(prefix);
    try (CloseableIterator<SchemaRegistryValue> allVersions = allVersions(prefix, true)) {
      Map<String, List<ExtendedSchema>> schemas = schemasByVersion(
          aliases, allVersions, filter, returnLatestOnly, postFilter);
      List<ExtendedSchema> result = new ArrayList<>();
      for (List<ExtendedSchema> schemaList : schemas.values()) {
        result.addAll(schemaList);
      }
      for (Map.Entry<String, List<String>> entry : aliases.entrySet()) {
        String subject = entry.getKey();
        if (!schemas.containsKey(subject)) {
          // Collect schemas for the aliased subject
          try (CloseableIterator<SchemaRegistryValue> subVersions = allVersions(subject, false)) {
            Map<String, List<ExtendedSchema>> subSchemas = schemasByVersion(
                aliases, subVersions, filter, returnLatestOnly, postFilter);
            for (List<ExtendedSchema> schemaList : subSchemas.values()) {
              result.addAll(schemaList);
            }
          }
        }
      }
      Collections.sort(result);
      return result.iterator();
    }
  }

  private Map<String, List<String>> getAliases(String subjectPrefix)
      throws SchemaRegistryException {
    try (CloseableIterator<SchemaRegistryValue> iter = allConfigs(subjectPrefix, true)) {
      Map<String, List<String>> subjectToAliases = new HashMap<>();
      while (iter.hasNext()) {
        ConfigValue configValue = (ConfigValue) iter.next();
        String alias = configValue.getAlias();
        if (alias == null) {
          continue;
        }
        // Use the subject of the configValue as the qualifying parent (not the subjectPrefix)
        QualifiedSubject qualAlias = QualifiedSubject.qualifySubjectWithParent(
            tenant(), configValue.getSubject(), alias, true);
        List<String> aliases = subjectToAliases.computeIfAbsent(
            qualAlias.toQualifiedSubject(), k -> new ArrayList<>());
        aliases.add(configValue.getSubject());
      }
      return subjectToAliases;
    }
  }

  private List<SchemaKey> getAllSchemaKeys(String subject)
      throws SchemaRegistryException {
    try (CloseableIterator<SchemaRegistryValue> allVersions = allVersions(subject, false)) {
      List<SchemaKey> schemaKeys = schemaKeysByVersion(allVersions, LookupFilter.INCLUDE_DELETED);
      Collections.sort(schemaKeys);
      return schemaKeys;
    }
  }

  @Override
  public Schema getLatestVersion(String subject) throws SchemaRegistryException {
    try (CloseableIterator<SchemaRegistryValue> allVersions = allVersions(subject, false)) {
      return getLatestVersionFromSubjectSchemas(allVersions);
    }
  }

  private Schema getLatestVersionFromSubjectSchemas(
          CloseableIterator<SchemaRegistryValue> schemas) {
    int latestVersionId = -1;
    SchemaValue latestSchemaValue = null;

    while (schemas.hasNext()) {
      SchemaValue schemaValue = (SchemaValue) schemas.next();
      if (schemaValue.isDeleted()) {
        continue;
      }
      if (schemaValue.getVersion() > latestVersionId) {
        latestVersionId = schemaValue.getVersion();
        latestSchemaValue = schemaValue;
      }
    }

    return latestSchemaValue != null ? toSchemaEntity(latestSchemaValue) : null;
  }

  private CloseableIterator<SchemaRegistryValue> allConfigs(
      String subjectOrPrefix, boolean isPrefix) throws SchemaRegistryException {
    return allVersions((s, v) -> new ConfigKey(s), subjectOrPrefix, isPrefix);
  }

  private CloseableIterator<SchemaRegistryValue> allVersions(
      String subjectOrPrefix, boolean isPrefix) throws SchemaRegistryException {
    return allVersions(SchemaKey::new, subjectOrPrefix, isPrefix);
  }

  private CloseableIterator<SchemaRegistryValue> allVersions(
      BiFunction<String, Integer, SchemaRegistryKey> keyCreator,
      String subjectOrPrefix, boolean isPrefix) throws SchemaRegistryException {
    try {
      String start;
      String end;
      int idx = subjectOrPrefix.indexOf(CONTEXT_WILDCARD);
      if (idx >= 0) {
        // Context wildcard match (prefix may contain tenant)
        String tenantPrefix = subjectOrPrefix.substring(0, idx);
        String unqualifiedSubjectOrPrefix =
            subjectOrPrefix.substring(idx + CONTEXT_WILDCARD.length());
        if (!unqualifiedSubjectOrPrefix.isEmpty()) {
          return allVersionsFromAllContexts(
              keyCreator, tenantPrefix, unqualifiedSubjectOrPrefix, isPrefix);
        }
        start = tenantPrefix + CONTEXT_PREFIX + CONTEXT_DELIMITER;
        end = tenantPrefix + CONTEXT_PREFIX + Character.MAX_VALUE + CONTEXT_DELIMITER;
      } else {
        start = subjectOrPrefix;
        end = isPrefix ? subjectOrPrefix + Character.MAX_VALUE : subjectOrPrefix;
      }
      SchemaRegistryKey key1 = keyCreator.apply(start, MIN_VERSION);
      SchemaRegistryKey key2 = keyCreator.apply(end, MAX_VERSION);
      return TransformedIterator.transform(kafkaStore.getAll(key1, key2), v -> {
        if (v instanceof SchemaValue) {
          metadataEncoder.decodeMetadata(((SchemaValue) v));
        }
        return v;
      });
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
          "Error from the backend Kafka store", e);
    }
  }

  private CloseableIterator<SchemaRegistryValue> allVersionsFromAllContexts(
      BiFunction<String, Integer, SchemaRegistryKey> keyCreator,
      String tenantPrefix, String unqualifiedSubjectOrPrefix, boolean isPrefix)
      throws SchemaRegistryException {
    List<SchemaRegistryValue> versions = new ArrayList<>();
    // Add versions from default context
    try (CloseableIterator<SchemaRegistryValue> iter =
        allVersions(keyCreator, tenantPrefix + unqualifiedSubjectOrPrefix, isPrefix)) {
      while (iter.hasNext()) {
        versions.add(iter.next());
      }
    }
    List<ContextValue> contexts = new ArrayList<>();
    try (CloseableIterator<SchemaRegistryValue> iter = allContexts()) {
      while (iter.hasNext()) {
        contexts.add((ContextValue) iter.next());
      }
    }
    for (ContextValue v : contexts) {
      QualifiedSubject qualSub =
          new QualifiedSubject(v.getTenant(), v.getContext(), unqualifiedSubjectOrPrefix);
      try (CloseableIterator<SchemaRegistryValue> subiter =
          allVersions(keyCreator, qualSub.toQualifiedSubject(), isPrefix)) {
        while (subiter.hasNext()) {
          versions.add(subiter.next());
        }
      }
    }
    return new DelegatingIterator<>(versions.iterator());
  }

  @Override
  public void close() throws IOException {
    log.info("Shutting down schema registry");
    kafkaStore.close();
    metadataEncoder.close();
    if (leaderElector != null) {
      leaderElector.close();
    }
    if (leaderRestService != null) {
      leaderRestService.close();
    }
  }

  public void updateConfig(String subject, Config config)
      throws SchemaRegistryStoreException, OperationNotPermittedException, UnknownLeaderException {
    if (isReadOnlyMode(subject)) {
      throw new OperationNotPermittedException("Subject " + subject + " is in read-only mode");
    }
    ConfigKey configKey = new ConfigKey(subject);
    try {
      kafkaStore.waitUntilKafkaReaderReachesLastOffset(subject, kafkaStoreTimeoutMs);
      ConfigValue oldConfig = (ConfigValue) kafkaStore.get(configKey);
      ConfigValue newConfig = new ConfigValue(subject, config, ruleSetHandler);
      kafkaStore.put(configKey, ConfigValue.update(oldConfig, newConfig));
      log.debug("Wrote new config: {} to the Kafka data store with key {}", config, configKey);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Failed to write new config value to the store",
                                             e);
    }
  }

  public void updateConfigOrForward(String subject, Config newConfig,
                                    Map<String, String> headerProperties)
      throws SchemaRegistryStoreException, SchemaRegistryRequestForwardingException,
      UnknownLeaderException, OperationNotPermittedException {
    kafkaStore.lockFor(subject).lock();
    try {
      if (isLeader()) {
        updateConfig(subject, newConfig);
      } else {
        // forward update config request to the leader
        if (leaderIdentity != null) {
          forwardUpdateConfigRequestToLeader(subject, newConfig, headerProperties);
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
      throw new OperationNotPermittedException("Subject " + subject + " is in read-only mode");
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

  private String kafkaClusterId(SchemaRegistryConfig config) throws SchemaRegistryException {
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

  public Config getConfig(String subject)
      throws SchemaRegistryStoreException {
    try {
      return lookupCache.config(subject, false, new Config(defaultCompatibilityLevel.name));
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Failed to write new config value to the store", e);
    }
  }

  public Config getConfigInScope(String subject)
      throws SchemaRegistryStoreException {
    try {
      return lookupCache.config(subject, true, new Config(defaultCompatibilityLevel.name));
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Failed to write new config value to the store", e);
    }
  }

  /**
   * @param previousSchemas Full schema history in chronological order
   */
  @Override
  public List<String> isCompatible(String subject,
                                   Schema newSchema,
                                   List<SchemaKey> previousSchemas,
                                   boolean normalize)
      throws SchemaRegistryException {

    if (previousSchemas == null) {
      log.error("Previous schema not provided");
      throw new InvalidSchemaException("Previous schema not provided");
    }

    try {
      List<ParsedSchemaHolder> prevParsedSchemas = new ArrayList<>(previousSchemas.size());
      for (SchemaKey previousSchema : previousSchemas) {
        prevParsedSchemas.add(new LazyParsedSchemaHolder(this, previousSchema));
      }

      Config config = getConfigInScope(subject);
      ParsedSchema parsedSchema = canonicalizeSchema(newSchema, config, true, normalize);
      if (parsedSchema == null) {
        log.error("Empty schema");
        throw new InvalidSchemaException("Empty schema");
      }
      return isCompatibleWithPrevious(config, parsedSchema, prevParsedSchemas);
    } catch (IllegalStateException e) {
      if (e.getCause() instanceof SchemaRegistryException) {
        throw (SchemaRegistryException) e.getCause();
      }
      throw e;
    }
  }

  private List<String> isCompatibleWithPrevious(Config config,
                                                ParsedSchema parsedSchema,
                                                List<ParsedSchemaHolder> previousSchemas) {
    List<String> errorMessages = new ArrayList<>();
    ParsedSchemaHolder previousSchemaHolder = !previousSchemas.isEmpty()
                                                  ? previousSchemas.get(previousSchemas.size() - 1)
                                                  : null;
    if (isSchemaFieldValidationEnabled(config)) {
      errorMessages.addAll(validateReservedFields(parsedSchema, previousSchemaHolder));
    }
    CompatibilityLevel compatibility = CompatibilityLevel.forName(config.getCompatibilityLevel());
    String compatibilityGroup = config.getCompatibilityGroup();
    if (compatibilityGroup != null) {
      String groupValue = getCompatibilityGroupValue(parsedSchema, compatibilityGroup);
      // Only check compatibility against schemas with the same compatibility group value,
      // which may be null.
      previousSchemas = previousSchemas.stream()
          .filter(s -> Objects.equals(groupValue,
              getCompatibilityGroupValue(s.schema(), compatibilityGroup)))
          .collect(Collectors.toList());
    }
    errorMessages.addAll(parsedSchema.isCompatible(compatibility, previousSchemas));
    if (!errorMessages.isEmpty()) {
      try {
        errorMessages.add(String.format("{validateFields: '%b', compatibility: '%s'}",
            isSchemaFieldValidationEnabled(config),
            compatibility));
      } catch (UnsupportedOperationException e) {
        // Ignore and return errorMessages
        log.warn("Failed to append 'compatibility' to error messages");
      }
    }
    return errorMessages;
  }

  private List<String> validateReservedFields(ParsedSchema currentSchema,
                                              ParsedSchemaHolder previousSchema) {
    List<String> errorMessages = new ArrayList<>();
    Set<String> updatedReservedFields = currentSchema.getReservedFields();
    if (previousSchema != null) {
      // check to ensure that original reserved fields are not removed in the updated version
      Sets.SetView<String> removedFields =
          Sets.difference(previousSchema.schema().getReservedFields(), updatedReservedFields);
      if (!removedFields.isEmpty()) {
        removedFields.forEach(field -> errorMessages.add(String.format(RESERVED_FIELD_REMOVED,
            field)));
      }
    }
    updatedReservedFields.forEach(reservedField -> {
      // check if updated fields conflict with reserved fields
      if (currentSchema.hasTopLevelField(reservedField)) {
        errorMessages.add(String.format(FIELD_CONFLICTS_WITH_RESERVED_FIELD, reservedField));
      }
    });
    return errorMessages;
  }

  private static String getCompatibilityGroupValue(
      ParsedSchema parsedSchema, String compatibilityGroup) {
    if (parsedSchema.metadata() != null && parsedSchema.metadata().getProperties() != null) {
      return parsedSchema.metadata().getProperties().get(compatibilityGroup);
    }
    return null;
  }

  private void deleteMode(String subject) throws StoreException {
    ModeKey modeKey = new ModeKey(subject);
    this.kafkaStore.delete(modeKey);
  }

  private void deleteConfig(String subject) throws StoreException {
    ConfigKey configKey = new ConfigKey(subject);
    this.kafkaStore.delete(configKey);
  }

  public Mode getMode(String subject) throws SchemaRegistryStoreException {
    try {
      Mode globalMode = lookupCache.mode(null, false, defaultMode);
      Mode subjectMode = lookupCache.mode(subject, false, defaultMode);

      return globalMode == Mode.READONLY_OVERRIDE ? globalMode : subjectMode;
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Failed to write new config value to the store", e);
    }
  }

  public Mode getModeInScope(String subject) throws SchemaRegistryStoreException {
    try {
      Mode globalMode = lookupCache.mode(null, true, defaultMode);
      Mode subjectMode = lookupCache.mode(subject, true, defaultMode);

      return globalMode == Mode.READONLY_OVERRIDE ? globalMode : subjectMode;
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Failed to write new config value to the store", e);
    }
  }

  public void setMode(String subject, Mode mode) throws SchemaRegistryException {
    setMode(subject, mode, false);
  }

  public void setMode(String subject, Mode mode, boolean force)
      throws SchemaRegistryException {
    if (!allowModeChanges) {
      throw new OperationNotPermittedException("Mode changes are not allowed");
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
      kafkaStore.put(modeKey, new ModeValue(subject, mode));
      log.debug("Wrote new mode: {} to the Kafka data store with key {}", mode.name(), modeKey);
    } catch (StoreTimeoutException te) {
      throw new SchemaRegistryTimeoutException("Write to the Kafka store timed out while", te);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Failed to write new mode to the store", e);
    }
  }

  public void setModeOrForward(String subject, Mode mode, boolean force,
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

  KafkaStore<SchemaRegistryKey, SchemaRegistryValue> getKafkaStore() {
    return this.kafkaStore;
  }

  private Map<String, List<ExtendedSchema>> schemasByVersion(
      Map<String, List<String>> subjectByAliases,
      CloseableIterator<SchemaRegistryValue> schemas,
      LookupFilter filter,
      boolean returnLatestOnly,
      Predicate<Schema> postFilter) {
    Map<String, List<ExtendedSchema>> schemaMap = new HashMap<>();
    ExtendedSchema previousSchema = null;
    while (schemas.hasNext()) {
      SchemaValue schemaValue = (SchemaValue) schemas.next();
      boolean shouldInclude = shouldInclude(schemaValue.isDeleted(), filter);
      if (!shouldInclude) {
        continue;
      }
      List<String> aliases = subjectByAliases.get(schemaValue.getSubject());
      ExtendedSchema schema = new ExtendedSchema(toSchemaEntity(schemaValue), aliases);
      List<ExtendedSchema> schemaList = schemaMap.computeIfAbsent(
          schemaValue.getSubject(), k -> new ArrayList<>());
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
      List<ExtendedSchema> schemaList = schemaMap.computeIfAbsent(
          previousSchema.getSubject(), k -> new ArrayList<>());
      schemaList.add(previousSchema);
    }
    if (postFilter != null) {
      for (Map.Entry<String, List<ExtendedSchema>> entry : schemaMap.entrySet()) {
        List<ExtendedSchema> schemaList = entry.getValue();
        schemaList = schemaList.stream()
            .filter(postFilter)
            .collect(Collectors.toList());
        entry.setValue(schemaList);
      }
    }
    return schemaMap;
  }

  private List<SchemaKey> schemaKeysByVersion(CloseableIterator<SchemaRegistryValue> schemas,
      LookupFilter filter) {
    List<SchemaKey> schemaList = new ArrayList<>();
    while (schemas.hasNext()) {
      SchemaValue schemaValue = (SchemaValue) schemas.next();
      boolean shouldInclude = shouldInclude(schemaValue.isDeleted(), filter);
      if (!shouldInclude) {
        continue;
      }
      SchemaKey schemaKey = schemaValue.toKey();
      schemaList.add(schemaKey);
    }
    return schemaList;
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

  private static boolean shouldInclude(boolean isDeleted, LookupFilter filter) {
    switch (filter) {
      case DEFAULT:
        return !isDeleted;
      case INCLUDE_DELETED:
        return true;
      case DELETED_ONLY:
        return isDeleted;
      default:
        return false;
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

  private boolean isSchemaFieldValidationEnabled(Config config) {
    return config.isValidateFields() != null ? config.isValidateFields() : defaultValidateFields;
  }

  private static class RawSchema {
    private final Schema schema;
    private final boolean isNew;
    private final boolean normalize;

    public RawSchema(Schema schema, boolean isNew, boolean normalize) {
      this.schema = schema;
      this.isNew = isNew;
      this.normalize = normalize;
    }

    public Schema getSchema() {
      return schema;
    }

    public boolean isNew() {
      return isNew;
    }

    public boolean isNormalize() {
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
      RawSchema that = (RawSchema) o;
      return isNew == that.isNew
          && normalize == that.normalize
          && Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(schema, isNew, normalize);
    }

    @Override
    public String toString() {
      return "RawSchema{"
          + "schema=" + schema
          + ", isNew=" + isNew
          + ", normalize=" + normalize
          + '}';
    }
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
