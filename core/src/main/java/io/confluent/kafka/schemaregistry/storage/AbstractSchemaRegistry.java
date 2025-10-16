/*
 * Copyright 2025 Confluent Inc.
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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Sets;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.CompatibilityPolicy;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.ParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.ContextId;
import io.confluent.kafka.schemaregistry.client.rest.entities.ExtendedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaTags;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.TagSchemaRequest;
import io.confluent.kafka.schemaregistry.exceptions.OperationNotPermittedException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.client.security.SslFactory;
import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.metrics.MetricsContainer;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.VersionId;
import io.confluent.kafka.schemaregistry.rest.extensions.SchemaRegistryResourceExtension;
import io.confluent.kafka.schemaregistry.rest.handlers.CompositeUpdateRequestHandler;
import io.confluent.kafka.schemaregistry.rest.handlers.UpdateRequestHandler;
import io.confluent.kafka.schemaregistry.storage.encoder.MetadataEncoderService;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.confluent.rest.NamedURI;
import io.confluent.rest.RestConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Time;
import org.eclipse.jetty.server.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import java.security.KeyStore;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.confluent.kafka.schemaregistry.client.rest.entities.Metadata.mergeMetadata;
import static io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet.mergeRuleSets;
import static io.confluent.kafka.schemaregistry.storage.FilteredIterator.filter;
import static io.confluent.kafka.schemaregistry.storage.TransformedIterator.transform;
import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.CONTEXT_DELIMITER;
import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.CONTEXT_PREFIX;
import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.CONTEXT_WILDCARD;
import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.DEFAULT_CONTEXT;

/**
 * Abstract base class for SchemaRegistry implementations that provides common state management
 * and shared functionality.
 */
public abstract class AbstractSchemaRegistry implements SchemaRegistry,
        SslFactory.SslFactoryCreated {

  private static final Logger log = LoggerFactory.getLogger(AbstractSchemaRegistry.class);

  protected Store<SchemaRegistryKey, SchemaRegistryValue> store;

  protected final SchemaRegistryConfig config;
  protected final Map<String, Object> props;
  protected final MetricsContainer metricsContainer;
  protected final Map<String, SchemaProvider> providers;
  protected final List<SchemaRegistryResourceExtension> resourceExtensions;
  protected final List<Handler.Singleton> customHandler;
  protected final List<UpdateRequestHandler> updateRequestHandlers;
  protected final SslFactory sslFactory;
  protected final LoadingCache<RawSchema, ParsedSchema> newSchemaCache;
  protected final LoadingCache<RawSchema, ParsedSchema> oldSchemaCache;
  protected final CompatibilityLevel defaultCompatibilityLevel;
  protected final boolean defaultValidateFields;
  protected final Mode defaultMode;
  protected final int schemaSearchDefaultLimit;
  protected final int schemaSearchMaxLimit;
  protected final int subjectVersionSearchDefaultLimit;
  protected final int subjectVersionSearchMaxLimit;
  protected final int subjectSearchDefaultLimit;
  protected final int contextSearchMaxLimit;
  protected final int contextSearchDefaultLimit;
  protected final int subjectSearchMaxLimit;
  protected final boolean allowModeChanges;
  protected final AtomicBoolean initialized;
  protected final Time time;

  protected LookupCache<SchemaRegistryKey, SchemaRegistryValue> lookupCache;
  protected MetadataEncoderService metadataEncoder;
  protected RuleSetHandler ruleSetHandler;

  /**
   * Constructs an AbstractSchemaRegistry with the given configuration.
   *
   * @param config the schema registry configuration
   * @param metricsContainer the metrics container for tracking metrics
   */
  protected AbstractSchemaRegistry(SchemaRegistryConfig config, MetricsContainer metricsContainer)
          throws SchemaRegistryException {
    this.config = config;
    this.props = new ConcurrentHashMap<>();
    this.metricsContainer = metricsContainer;
    this.providers = initProviders();
    this.resourceExtensions = initResourceExtensions();
    this.customHandler = new CopyOnWriteArrayList<>();
    this.updateRequestHandlers = new CopyOnWriteArrayList<>();
    this.ruleSetHandler = new RuleSetHandler();
    String interInstanceListenerNameConfig = config.interInstanceListenerName();
    NamedURI internalListener = getInterInstanceListener(config.getListeners(),
            interInstanceListenerNameConfig,
            config.interInstanceProtocol());
    Map<String, Object> sslConfig = config.getOverriddenSslConfigs(internalListener);
    this.sslFactory =
            new SslFactory(ConfigDef.convertToStringMapWithPasswordValues(sslConfig), this);
    this.newSchemaCache = Caffeine.newBuilder()
            .maximumSize(config.getInt(SchemaRegistryConfig.SCHEMA_CACHE_SIZE_CONFIG) / 2)
            .expireAfterAccess(config.getInt(SchemaRegistryConfig.SCHEMA_CACHE_EXPIRY_SECS_CONFIG),
                    TimeUnit.SECONDS)
            .build(s -> loadSchema(s.getSchema(), s.isNew(), s.isNormalize()));
    this.oldSchemaCache = Caffeine.newBuilder()
            .maximumSize(config.getInt(SchemaRegistryConfig.SCHEMA_CACHE_SIZE_CONFIG) / 2)
            .expireAfterAccess(config.getInt(SchemaRegistryConfig.SCHEMA_CACHE_EXPIRY_SECS_CONFIG),
                    TimeUnit.SECONDS)
            .build(s -> loadSchema(s.getSchema(), s.isNew(), s.isNormalize()));
    this.defaultCompatibilityLevel = config.compatibilityType();
    this.defaultValidateFields =
        config.getBoolean(SchemaRegistryConfig.SCHEMA_VALIDATE_FIELDS_CONFIG);
    this.defaultMode = Mode.READWRITE;
    this.schemaSearchDefaultLimit =
        config.getInt(SchemaRegistryConfig.SCHEMA_SEARCH_DEFAULT_LIMIT_CONFIG);
    this.schemaSearchMaxLimit =
        config.getInt(SchemaRegistryConfig.SCHEMA_SEARCH_MAX_LIMIT_CONFIG);
    this.subjectVersionSearchDefaultLimit =
        config.getInt(SchemaRegistryConfig.SUBJECT_VERSION_SEARCH_DEFAULT_LIMIT_CONFIG);
    this.subjectVersionSearchMaxLimit =
        config.getInt(SchemaRegistryConfig.SUBJECT_VERSION_SEARCH_MAX_LIMIT_CONFIG);
    this.subjectSearchDefaultLimit =
        config.getInt(SchemaRegistryConfig.SUBJECT_SEARCH_DEFAULT_LIMIT_CONFIG);
    this.contextSearchMaxLimit =
        config.getInt(SchemaRegistryConfig.CONTEXT_SEARCH_MAX_LIMIT_CONFIG);
    this.contextSearchDefaultLimit =
        config.getInt(SchemaRegistryConfig.CONTEXT_SEARCH_DEFAULT_LIMIT_CONFIG);
    this.subjectSearchMaxLimit =
        config.getInt(SchemaRegistryConfig.SUBJECT_SEARCH_MAX_LIMIT_CONFIG);
    this.allowModeChanges = config.getBoolean(SchemaRegistryConfig.MODE_MUTABILITY);
    this.initialized = new AtomicBoolean(false);
    this.time = config.getTime();
  }

  protected List<SchemaRegistryResourceExtension> initResourceExtensions() {
    return config.getConfiguredInstances(
        config.definedResourceExtensionConfigName(),
        SchemaRegistryResourceExtension.class);
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
      if (listener.getName() != null
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

  protected Map<String, SchemaProvider> initProviders() {
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

  protected void registerProviders(
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

  /**
   * Loads a schema from the cache or parses it if not cached.
   */
  protected ParsedSchema loadSchema(Schema schema, boolean isNew, boolean normalize)
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
      throw new InvalidSchemaException("Invalid schema of type " + type
              + ", details: " + e.getMessage());
    }
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

      return register(subject, schema, normalize, request.doPropagateSchemaTags());
    } catch (IllegalArgumentException e) {
      throw new InvalidSchemaException(e);
    }
  }

  protected boolean isReadOnlyMode(String subject) throws SchemaRegistryStoreException {
    Mode subjectMode = getModeInScope(subject);
    return subjectMode == Mode.READONLY || subjectMode == Mode.READONLY_OVERRIDE;
  }

  protected void checkRegisterMode(
          String subject, Schema schema
  ) throws OperationNotPermittedException, SchemaRegistryStoreException {
    String context = QualifiedSubject.qualifiedContextFor(tenant(), subject);
    if (isReadOnlyMode(subject)) {
      throw new OperationNotPermittedException("Subject " + subject
              + " in context " + context + " is in read-only mode");
    }

    if (schema.getId() >= 0) {
      if (!getModeInScope(subject).isImportOrForwardMode()) {
        throw new OperationNotPermittedException("Subject " + subject
                + " in context " + context + " is not in import mode");
      }
    } else {
      if (getModeInScope(subject) != Mode.READWRITE) {
        throw new OperationNotPermittedException(
                "Subject " + subject + " in context "
                        + context + " is not in read-write mode"
        );
      }
    }
  }

  private boolean maybePropagateSchemaTags(
          Schema schema, LazyParsedSchemaHolder previousSchema, boolean propagateSchemaTags)
          throws InvalidSchemaException {
    if (!propagateSchemaTags || previousSchema == null) {
      return false;
    }
    Map<SchemaEntity, Set<String>> schemaTags = previousSchema.schema().inlineTaggedEntities();
    if (schemaTags.isEmpty()) {
      return false;
    }
    ParsedSchema parsedSchema = parseSchema(schema);
    parsedSchema = parsedSchema.copy(schemaTags, Collections.emptyMap());
    schema.setSchema(parsedSchema.canonicalString());
    return true;
  }

  private String getConfluentVersion(Metadata metadata) {
    return metadata != null ? metadata.getConfluentVersion() : null;
  }

  protected boolean maybeSetMetadataRuleSet(
          Config config, Schema schema, Schema previousSchema, Integer newVersion) {
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
    if (newVersion != null
            && (schema.getVersion() != 0 || getConfluentVersion(mergedMetadata) != null)) {
      mergedMetadata = Metadata.setConfluentVersion(mergedMetadata, newVersion);
    }

    if (mergedMetadata != null || mergedRuleSet != null) {
      schema.setMetadata(mergedMetadata);
      schema.setRuleSet(mergedRuleSet);
      return true;
    }
    return false;
  }

  protected boolean maybePopulateFromPrevious(
          Config config, Schema schema, List<ParsedSchemaHolder> undeletedVersions, int newVersion,
          boolean propagateSchemaTags)
          throws SchemaRegistryException {
    boolean populatedSchema = false;
    LazyParsedSchemaHolder previousSchemaHolder = !undeletedVersions.isEmpty()
            ? (LazyParsedSchemaHolder) undeletedVersions.get(0)
            : null;
    Schema previousSchema = previousSchemaHolder != null
            ? toSchemaEntity(previousSchemaHolder.schemaValue())
            : null;
    if (schema == null
            || schema.getSchema() == null
            || schema.getSchema().trim().isEmpty()) {
      if (previousSchema != null) {
        schema.setSchema(previousSchema.getSchema());
        schema.setSchemaType(previousSchema.getSchemaType());
        schema.setReferences(previousSchema.getReferences());
        populatedSchema = true;
      } else {
        throw new InvalidSchemaException("Empty schema");
      }
    }
    boolean populatedSchemaTags = maybePropagateSchemaTags(
            schema, previousSchemaHolder, propagateSchemaTags);
    boolean populatedMetadataRuleSet = maybeSetMetadataRuleSet(
            config, schema, previousSchema, newVersion);
    return populatedSchema || populatedSchemaTags || populatedMetadataRuleSet;
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
      List<io.confluent.kafka.schemaregistry.client.rest.entities.Rule> migrationRules =
              ruleSet.getMigrationRules();
      if (migrationRules != null) {
        migrationRules = migrationRules.stream()
                .filter(r -> !rulesToRemove.contains(r.getName()))
                .collect(Collectors.toList());
      }
      List<io.confluent.kafka.schemaregistry.client.rest.entities.Rule> domainRules =
              ruleSet.getDomainRules();
      if (domainRules != null) {
        domainRules = domainRules.stream()
                .filter(r -> !rulesToRemove.contains(r.getName()))
                .collect(Collectors.toList());
      }
      List<Rule> encodingRules = ruleSet.getEncodingRules();
      if (encodingRules != null) {
        encodingRules = encodingRules.stream()
                .filter(r -> !rulesToRemove.contains(r.getName()))
                .collect(Collectors.toList());
      }
      ruleSet = new RuleSet(migrationRules, domainRules, encodingRules);
    }
    return ruleSet;
  }

  public Schema modifySchemaTags(String subject, Schema schema, TagSchemaRequest request)
          throws SchemaRegistryException {
    ParsedSchema parsedSchema = parseSchema(schema);
    int newVersion = request.getNewVersion() != null ? request.getNewVersion() : 0;

    Metadata mergedMetadata = request.getMetadata() != null
            ? request.getMetadata()
            : parsedSchema.metadata();
    mergedMetadata = Metadata.setConfluentVersion(mergedMetadata, newVersion);

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

  protected void logSchemaOp(Schema schema, String operation) {
    log.info("Resource association log - (tenant, id, subject, operation): ({}, {}, {}, {})",
            tenant(), schema.getId(), schema.getSubject(), operation);
  }

  private boolean isSchemaFieldValidationEnabled(Config config) {
    return config.isValidateFields() != null ? config.isValidateFields() : defaultValidateFields;
  }

  private ParsedSchema maybeValidateAndNormalizeSchema(ParsedSchema parsedSchema,
                                                       Schema schema,
                                                       Config config,
                                                       boolean normalize)
          throws InvalidSchemaException {
    try {
      Mode mode = getModeInScope(schema.getSubject());
      if (!mode.isImportOrForwardMode()) {
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

  protected ParsedSchema canonicalizeSchema(Schema schema,
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

  protected boolean isSubjectVersionDeleted(String subject, int version)
          throws SchemaRegistryException {
    try {
      SchemaValue schemaValue = (SchemaValue) this.store.get(new SchemaKey(subject, version));
      return schemaValue == null || schemaValue.isDeleted();
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
              "Error while retrieving schema from the backend Kafka"
                      + " store", e);
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
      return filter(transform(store.getAll(key1, key2), v -> {
        if (v instanceof SchemaValue) {
          try {
            metadataEncoder.decodeMetadata(((SchemaValue) v));
          } catch (SchemaRegistryStoreException e) {
            log.error("Failed to decode metadata for schema id {}", ((SchemaValue) v).getId(), e);
            return null;
          }
        }
        return v;
      }), Objects::nonNull);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
              "Error from the backend Kafka store", e);
    }
  }

  private CloseableIterator<SchemaRegistryValue> allVersions(
          String subjectOrPrefix, boolean isPrefix) throws SchemaRegistryException {
    return allVersions(SchemaKey::new, subjectOrPrefix, isPrefix);
  }

  private static boolean shouldInclude(boolean isDeleted, LookupFilter filter) {
    return switch (filter) {
      case DEFAULT -> !isDeleted;
      case INCLUDE_DELETED -> true;
      case DELETED_ONLY -> isDeleted;
    };
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

  protected List<SchemaKey> getAllSchemaKeysDescending(String subject)
          throws SchemaRegistryException {
    try (CloseableIterator<SchemaRegistryValue> allVersions = allVersions(subject, false)) {
      List<SchemaKey> schemaKeys = schemaKeysByVersion(allVersions, LookupFilter.INCLUDE_DELETED);
      schemaKeys.sort(Collections.reverseOrder());
      return schemaKeys;
    }
  }

  protected Schema lookUpSchemaUnderSubject(
          Config config, String subject, Schema schema, boolean normalize,
          boolean lookupDeletedSchema, boolean lookupLatestOnly) throws SchemaRegistryException {
    try {
      // Pass a copy of the schema so the original is not modified during normalization
      // to ensure that invalid defaults are not dropped since default validation is disabled
      Schema newSchema = schema != null ? schema.copy() : null;
      ParsedSchema parsedSchema = canonicalizeSchema(newSchema, config, false, normalize);
      if (parsedSchema != null && !lookupLatestOnly) {
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

      if (lookupLatestOnly) {
        Schema prev = getLatestVersion(subject);
        if (prev != null
                && parsedSchema != null
                && parsedSchema.canLookup(parseSchema(prev), this)) {
          // This handles the case where a schema is sent with all references resolved
          // or without confluent:version
          return prev;
        }
      } else {
        List<SchemaKey> allVersions = getAllSchemaKeysDescending(subject);

        for (SchemaKey schemaKey : allVersions) {
          Schema prev = get(schemaKey.getSubject(), schemaKey.getVersion(), lookupDeletedSchema);
          if (prev != null
                  && parsedSchema != null
                  && parsedSchema.canLookup(parseSchema(prev), this)) {
            // This handles the case where a schema is sent with all references resolved
            // or without confluent:version
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

  /**
   * Checks if given schema was ever registered under a subject. If found, it returns the version of
   * the schema under the subject. If not, returns -1
   */
  @Override
  public Schema lookUpSchemaUnderSubject(
          String subject, Schema schema, boolean normalize, boolean lookupDeletedSchema)
          throws SchemaRegistryException {
    if (schema == null) {
      return null;
    }
    Config config = getConfigInScope(subject);
    Schema existingSchema = lookUpSchemaUnderSubject(
            config, subject, schema, normalize, lookupDeletedSchema, false);
    if (existingSchema != null) {
      return existingSchema;
    }
    Schema prev = getLatestVersion(subject);
    if (prev == null) {
      return null;
    }
    Schema next = schema.copy();
    // If a previous schema is available, possibly populate the new schema with the
    // metadata and rule set and perform another lookup.
    // This mimics the additional lookup during schema registration.
    maybeSetMetadataRuleSet(config, next, prev, null);
    if (next.equals(schema)) {
      return null;
    }
    return lookUpSchemaUnderSubject(
            config, subject, next, normalize, lookupDeletedSchema, false);
  }

  public void checkIfSchemaWithIdExist(int id, Schema schema)
          throws SchemaRegistryException, StoreException {
    String qctx = QualifiedSubject.qualifiedContextFor(tenant(), schema.getSubject());
    SchemaKey existingKey = this.lookupCache.schemaKeyById(id, qctx);
    if (existingKey != null) {
      SchemaRegistryValue existingValue = this.lookupCache.get(existingKey);
      if (existingValue instanceof SchemaValue existingSchemaValue) {
        Schema existingSchema = toSchemaEntity(existingSchemaValue);
        Schema schemaCopy = schema.copy();
        schemaCopy.setId(existingSchema.getId());
        schemaCopy.setSubject(existingSchema.getSubject());
        schemaCopy.setVersion(existingSchema.getVersion());
        if (!existingSchema.equals(schemaCopy)) {
          String context = QualifiedSubject.qualifiedContextFor(tenant(), schema.getSubject());
          throw new OperationNotPermittedException(
                  String.format("Overwrite new schema with id %s in context"
                                  + " %s is not permitted.",
                          id, context)
          );
        }
      }
    }
  }

  protected CloseableIterator<SchemaRegistryValue> allContexts() throws SchemaRegistryException {
    try {
      ContextKey key1 = new ContextKey(tenant(), String.valueOf(Character.MIN_VALUE));
      ContextKey key2 = new ContextKey(tenant(), String.valueOf(Character.MAX_VALUE));
      return lookupCache.getAll(key1, key2);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
              "Error from the backend Kafka store", e);
    }
  }

  SchemaKey getSchemaKeyUsingContexts(int id, String subject)
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

  private Map.Entry<Integer, String> getIdUsingContexts(String guid)
          throws StoreException, SchemaRegistryException {
    Integer id = lookupCache.idByGuid(guid, DEFAULT_CONTEXT);
    if (id != null) {
      return new AbstractMap.SimpleEntry<>(id, DEFAULT_CONTEXT);
    }
    try (CloseableIterator<SchemaRegistryValue> iter = allContexts()) {
      while (iter.hasNext()) {
        ContextValue v = (ContextValue) iter.next();
        QualifiedSubject qualSub = new QualifiedSubject(v.getTenant(), v.getContext(), null);
        String ctx = qualSub.toQualifiedContext();
        id = lookupCache.idByGuid(guid, ctx);
        if (id != null) {
          return new AbstractMap.SimpleEntry<>(id, ctx);
        }
      }
    }
    return null;
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

  public List<SubjectVersion> listVersionsForId(int id, String subject)
          throws SchemaRegistryException {
    return listVersionsForId(id, subject, false);
  }

  @Override
  public List<SubjectVersion> listVersionsForId(int id, String subject, boolean lookupDeleted)
          throws SchemaRegistryException {
    SchemaValue schema;
    try {
      SchemaKey subjectVersionKey = getSchemaKeyUsingContexts(id, subject);
      if (subjectVersionKey == null) {
        return null;
      }
      schema = (SchemaValue) store.get(subjectVersionKey);
      if (schema == null) {
        return null;
      }
      Schema schemaEntity = toSchemaEntity(schema);
      logSchemaOp(schemaEntity, "READ");

      SchemaIdAndSubjects schemaIdAndSubjects =
              this.lookupCache.schemaIdAndSubjects(schemaEntity);
      if (schemaIdAndSubjects == null) {
        return null;
      }
      return schemaIdAndSubjects
              .allSubjectVersions()
              .entrySet()
              .stream()
              .flatMap(e -> {
                try {
                  SchemaValue schemaValue =
                          (SchemaValue) store.get(new SchemaKey(e.getKey(), e.getValue()));
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

  private CloseableIterator<SchemaRegistryValue> allConfigs(
          String subjectOrPrefix, boolean isPrefix) throws SchemaRegistryException {
    return allVersions((s, v) -> new ConfigKey(s), subjectOrPrefix, isPrefix);
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
      Schema schemaEntity;
      try {
        schemaEntity = toSchemaEntity(schemaValue);
      } catch (SchemaRegistryStoreException e) {
        log.error("Failed to decode metadata for schema id {}", schemaValue.getId(), e);
        continue;
      }
      List<String> aliases = subjectByAliases.get(schemaValue.getSubject());
      ExtendedSchema schema = new ExtendedSchema(schemaEntity, aliases);
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

  public Iterator<ExtendedSchema> allVersionsIncludingAliasesWithSubjectPrefix(
          String prefix, LookupFilter filter, boolean returnLatestOnly,
          Predicate<Schema> postFilter) throws SchemaRegistryException {
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

  private Schema getLatestVersionFromSubjectSchemas(
          CloseableIterator<SchemaRegistryValue> schemas) throws SchemaRegistryException {
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

  protected List<String> isCompatibleWithPrevious(Config config,
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
    CompatibilityPolicy compatibilityPolicy =
            CompatibilityPolicy.forName(config.getCompatibilityPolicy());
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
    errorMessages.addAll(
            parsedSchema.isCompatible(compatibility, compatibilityPolicy, previousSchemas));
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

  public void removeCustomHandler(Handler.Singleton handler) {
    customHandler.remove(handler);
  }

  public int normalizeLimit(int suppliedLimit, int defaultLimit, int maxLimit) {
    int limit = defaultLimit;
    if (suppliedLimit > 0 && suppliedLimit <= maxLimit) {
      limit = suppliedLimit;
    }
    return limit;
  }

  @Override
  public void clearNewSchemaCache() {
    newSchemaCache.invalidateAll();
  }

  @Override
  public void clearOldSchemaCache() {
    oldSchemaCache.invalidateAll();
  }

  @Override
  public void invalidateFromNewSchemaCache(Schema schemaKey) {
    newSchemaCache.invalidate(new RawSchema(schemaKey, true, false));
    newSchemaCache.invalidate(new RawSchema(schemaKey, true, true));
  }

  protected void invalidateFromOldSchemaCache(Schema schemaKey) {
    oldSchemaCache.invalidate(new RawSchema(schemaKey, false, false));
    oldSchemaCache.invalidate(new RawSchema(schemaKey, false, true));
  }

  @Override
  public int normalizeSchemaLimit(int suppliedLimit) {
    return normalizeLimit(suppliedLimit, schemaSearchDefaultLimit, schemaSearchMaxLimit);
  }

  @Override
  public int normalizeSubjectLimit(int suppliedLimit) {
    return normalizeLimit(suppliedLimit, subjectSearchDefaultLimit, subjectSearchMaxLimit);
  }

  @Override
  public int normalizeContextLimit(int suppliedLimit) {
    return normalizeLimit(suppliedLimit, contextSearchDefaultLimit, contextSearchMaxLimit);
  }

  @Override
  public int normalizeSubjectVersionLimit(int suppliedLimit) {
    return normalizeLimit(suppliedLimit,
            subjectVersionSearchDefaultLimit, subjectVersionSearchMaxLimit);
  }

  @Override
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

  @Override
  public SchemaString get(int id, String subject) throws SchemaRegistryException {
    return get(id, subject, null, false);
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
  public void setMode(String subject, ModeUpdateRequest mode) throws SchemaRegistryException {
    setMode(subject, mode, false);
  }

  @Override
  public Mode getModeInScope(String subject) throws SchemaRegistryStoreException {
    try {
      Mode globalMode = lookupCache.mode(null, true, defaultMode);
      Mode subjectMode = lookupCache.mode(subject, true, defaultMode);

      return globalMode == Mode.READONLY_OVERRIDE ? globalMode : subjectMode;
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Failed to write new config value to the store", e);
    }
  }

  @Override
  public Mode getMode(String subject) throws SchemaRegistryStoreException {
    try {
      Mode globalMode = lookupCache.mode(null, false, defaultMode);
      Mode subjectMode = lookupCache.mode(subject, false, defaultMode);

      return globalMode == Mode.READONLY_OVERRIDE ? globalMode : subjectMode;
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

  @Override
  public Config getConfigInScope(String subject)
          throws SchemaRegistryStoreException {
    try {
      return lookupCache.config(subject, true, new Config(defaultCompatibilityLevel.name));
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Failed to write new config value to the store", e);
    }
  }

  @Override
  public Config getConfig(String subject)
          throws SchemaRegistryStoreException {
    try {
      return lookupCache.config(subject, false, new Config(defaultCompatibilityLevel.name));
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Failed to write new config value to the store", e);
    }
  }

  @Override
  public Schema getLatestVersion(String subject) throws SchemaRegistryException {
    try (CloseableIterator<SchemaRegistryValue> allVersions = allVersions(subject, false)) {
      return getLatestVersionFromSubjectSchemas(allVersions);
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

  @Override
  public Set<String> listSubjectsForId(int id, String subject) throws SchemaRegistryException {
    return listSubjectsForId(id, subject, false);
  }

  @Override
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
  public Set<String> listSubjectsWithPrefix(String prefix, LookupFilter filter)
          throws SchemaRegistryException {
    try (CloseableIterator<SchemaRegistryValue> allVersions = allVersions(prefix, true)) {
      return extractUniqueSubjects(allVersions, filter);
    }
  }

  @Override
  public Set<String> listSubjects(LookupFilter filter)
          throws SchemaRegistryException {
    return listSubjectsWithPrefix(CONTEXT_WILDCARD, filter);
  }

  @Override
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
  public List<ContextId> listIdsForGuid(String guid)
          throws SchemaRegistryException {
    try {
      List<ContextId> ids = new ArrayList<>();
      Integer id = lookupCache.idByGuid(guid, DEFAULT_CONTEXT);
      if (id != null) {
        ids.add(new ContextId(DEFAULT_CONTEXT, id));
      }
      try (CloseableIterator<SchemaRegistryValue> iter = allContexts()) {
        while (iter.hasNext()) {
          ContextValue v = (ContextValue) iter.next();
          QualifiedSubject qualSub = new QualifiedSubject(v.getTenant(), v.getContext(), null);
          String ctx = qualSub.toQualifiedContext();
          id = lookupCache.idByGuid(guid, ctx);
          if (id != null) {
            ids.add(new ContextId(qualSub.getContext(), id));
          }
        }
      }
      Collections.sort(ids);
      return ids;
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException("Error while retrieving schema with guid "
              + guid + " from the backend Kafka store", e);
    }
  }

  @Override
  public SchemaString getByGuid(String guid, String format) throws SchemaRegistryException {
    try {
      Map.Entry<Integer, String> id = getIdUsingContexts(guid);
      if (id == null) {
        return null;
      }
      SchemaString schema = get(id.getKey(), id.getValue(), format, false);
      schema.setSubject(null);
      schema.setVersion(null);
      return schema;
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
              "Error while retrieving schema with guid "
                      + guid
                      + " from the backend Kafka"
                      + " store", e);
    }
  }

  @Override
  public SchemaValue getSchemaValue(SchemaKey key)
          throws SchemaRegistryException {
    try {
      return (SchemaValue) store.get(key);
    } catch (StoreException e) {
      throw new SchemaRegistryStoreException(
              "Error while retrieving schema from the backend Kafka"
                      + " store", e);
    }
  }

  @Override
  public Schema toSchemaEntity(SchemaValue schemaValue) throws SchemaRegistryStoreException {
    metadataEncoder.decodeMetadata(schemaValue);
    return schemaValue.toSchemaEntity();
  }

  @Override
  public boolean schemaVersionExists(String subject, VersionId versionId, boolean
          returnDeletedSchema) throws SchemaRegistryException {
    final int version = versionId.getVersionId();
    Schema schema = this.get(subject, version, returnDeletedSchema);
    return (schema != null);
  }

  @Override
  public Schema getUsingContexts(String subject, int version, boolean
          returnDeletedSchema) throws SchemaRegistryException {
    Schema schema = get(subject, version, returnDeletedSchema);
    if (schema != null) {
      logSchemaOp(schema, "READ");
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
          logSchemaOp(schema, "READ");
          return schema;
        }
      }
    }
    return null;
  }

  @Override
  public ParsedSchema parseSchema(
          Schema schema,
          boolean isNew,
          boolean normalize) throws InvalidSchemaException {
    try {
      AbstractSchemaRegistry.RawSchema rawSchema =
              new AbstractSchemaRegistry.RawSchema(schema.toHashKey(), isNew, normalize);
      ParsedSchema parsedSchema = isNew
              ? newSchemaCache.get(rawSchema)
              : oldSchemaCache.get(rawSchema);
      if (schema.getVersion() != null) {
        parsedSchema = parsedSchema.copy(schema.getVersion());
      }
      return parsedSchema;
    } catch (Exception e) {
      Throwable cause = e.getCause();
      if (cause instanceof InvalidSchemaException) {
        throw (InvalidSchemaException) cause;
      } else {
        throw new InvalidSchemaException(e);
      }
    }
  }

  @Override
  public ParsedSchema parseSchema(Schema schema) throws InvalidSchemaException {
    return parseSchema(schema, false, false);
  }

  @Override
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

  @Override
  public Schema lookUpSchemaUnderSubjectUsingContexts(
          String subject, Schema schema, boolean normalize, boolean lookupDeletedSchema)
          throws SchemaRegistryException {
    Schema matchingSchema =
            lookUpSchemaUnderSubject(subject, schema, normalize, lookupDeletedSchema);
    if (matchingSchema != null) {
      logSchemaOp(matchingSchema, "READ");
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
        try {
          matchingSchema = lookUpSchemaUnderSubject(
                  qualSub.toQualifiedSubject(), qualSchema, normalize, lookupDeletedSchema);
        } catch (InvalidSchemaException e) {
          // ignore
        }
        if (matchingSchema != null) {
          logSchemaOp(matchingSchema, "READ");
          return matchingSchema;
        }
      }
    }
    return null;
  }

  @Override
  public Schema getLatestWithMetadata(
          String subject, Map<String, String> metadata, boolean lookupDeletedSchema)
          throws SchemaRegistryException {
    List<SchemaKey> allVersions = getAllSchemaKeysDescending(subject);

    for (SchemaKey schemaKey : allVersions) {
      Schema schema = get(schemaKey.getSubject(), schemaKey.getVersion(), lookupDeletedSchema);
      if (schema != null) {
        logSchemaOp(schema, "READ");
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

  @Override
  public LookupCache<SchemaRegistryKey, SchemaRegistryValue> getLookupCache() {
    return lookupCache;
  }

  @Override
  public Set<String> schemaTypes() {
    return providers.keySet();
  }

  @Override
  public SchemaRegistryConfig config() {
    return config;
  }

  @Override
  public Map<String, Object> properties() {
    return props;
  }

  @Override
  public MetadataEncoderService getMetadataEncoder() {
    return metadataEncoder;
  }

  @Override
  public void addUpdateRequestHandler(UpdateRequestHandler updateRequestHandler) {
    updateRequestHandlers.add(updateRequestHandler);
  }

  @Override
  public UpdateRequestHandler getCompositeUpdateRequestHandler() {
    List<UpdateRequestHandler> handlers = new ArrayList<>();
    handlers.add(ruleSetHandler);
    handlers.addAll(updateRequestHandlers);
    return new CompositeUpdateRequestHandler(handlers);
  }

  @Override
  public List<Handler.Singleton> getCustomHandler() {
    return customHandler;
  }

  @Override
  public void addCustomHandler(Handler.Singleton handler) {
    customHandler.add(handler);
  }

  @Override
  public MetricsContainer getMetricsContainer() {
    return metricsContainer;
  }

  @Override
  public List<SchemaRegistryResourceExtension> getResourceExtensions() {
    return resourceExtensions;
  }

  @Override
  public SchemaProvider schemaProvider(String schemaType) {
    return providers.get(schemaType);
  }

  @Override
  public RuleSetHandler getRuleSetHandler() {
    return ruleSetHandler;
  }

  @Override
  public void setRuleSetHandler(RuleSetHandler ruleSetHandler) {
    this.ruleSetHandler = ruleSetHandler;
  }

  @Override
  public SslFactory getSslFactory() {
    return sslFactory;
  }

  @Override
  public void onKeystoreCreated(KeyStore keystore) {
    metricsContainer.emitCertificateExpirationMetric(
            keystore, metricsContainer.getCertificateExpirationKeystore());
  }

  @Override
  public void onTruststoreCreated(KeyStore truststore) {
    metricsContainer.emitCertificateExpirationMetric(
            truststore, metricsContainer.getCertificateExpirationTruststore());
  }

  /**
   * Internal class representing a raw schema with parsing options.
   */
  protected static class RawSchema {
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
