/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.serializers;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.RULE_ACTIONS;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.rules.RulePhase;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.rules.DlqAction;
import io.confluent.kafka.schemaregistry.rules.ErrorAction;
import io.confluent.kafka.schemaregistry.rules.NoneAction;
import io.confluent.kafka.schemaregistry.rules.RuleAction;
import io.confluent.kafka.schemaregistry.rules.RuleConditionException;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.schemaregistry.rules.RuleExecutor;
import io.confluent.kafka.schemaregistry.rules.RuleBase;
import io.confluent.kafka.serializers.schema.id.DualSchemaIdDeserializer;
import io.confluent.kafka.serializers.schema.id.SchemaIdDeserializer;
import io.confluent.kafka.serializers.schema.id.SchemaIdSerializer;
import io.confluent.kafka.serializers.schema.id.PrefixSchemaIdSerializer;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.confluent.kafka.serializers.context.NullContextNameStrategy;
import io.confluent.kafka.serializers.context.strategy.ContextNameStrategy;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common fields and helper methods for both the serializer and the deserializer.
 */
public abstract class AbstractKafkaSchemaSerDe implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(AbstractKafkaSchemaSerDe.class);

  protected static final int DEFAULT_CACHE_CAPACITY = 1000;

  protected AbstractKafkaSchemaSerDeConfig config;
  protected Map<String, Object> configOriginals;
  protected SchemaRegistryClient schemaRegistry;
  protected Ticker ticker = Ticker.systemTicker();
  protected ContextNameStrategy contextNameStrategy = new NullContextNameStrategy();
  protected SubjectNameStrategy keySubjectNameStrategy = new TopicNameStrategy();
  protected SchemaIdSerializer keySchemaIdSerializer = new PrefixSchemaIdSerializer();
  protected SchemaIdDeserializer keySchemaIdDeserializer = new DualSchemaIdDeserializer();
  protected SubjectNameStrategy valueSubjectNameStrategy = new TopicNameStrategy();
  protected SchemaIdSerializer valueSchemaIdSerializer = new PrefixSchemaIdSerializer();
  protected SchemaIdDeserializer valueSchemaIdDeserializer = new DualSchemaIdDeserializer();
  protected Cache<SubjectSchema, ExtendedSchema> latestVersions;
  protected Cache<String, ExtendedSchema> latestWithMetadata;
  protected boolean useSchemaReflection;
  protected boolean useLatestVersion;
  protected Map<String, String> metadata;
  protected boolean enableRuleServiceLoader;
  protected Map<String, Map<String, RuleBase>> ruleExecutors;
  protected Map<String, Map<String, RuleBase>> ruleActions;
  protected boolean isKey;

  private Map<Rule, String> onSuccessActions;
  private Map<Rule, String> onFailureActions;
  private Map<Rule, Boolean> disabledFlags;

  private static final ErrorAction ERROR_ACTION = new ErrorAction();
  private static final NoneAction NONE_ACTION = new NoneAction();

  private static final String ON_SUCCESS = "onSuccess";
  private static final String ON_FAILURE = "onFailure";
  private static final String DISABLED = "disabled";

  private static final String PARAM = ".param.";

  // Track the key for use when deserializing/serializing the value, such as for a DLQ.
  // We take advantage of the fact the value serde is called after the key serde.
  private static final ThreadLocal<Object> key = new ThreadLocal<>();

  public static Object key() {
    return key.get();
  }

  public static void setKey(Object obj) {
    key.set(obj);
  }

  public static void clearKey() {
    key.remove();
  }

  protected Ticker ticker(SchemaRegistryClient client) {
    return client != null ? client.ticker() : Ticker.systemTicker();
  }

  @SuppressWarnings("unchecked")
  protected void configureClientProperties(
      AbstractKafkaSchemaSerDeConfig config,
      SchemaProvider provider) {
    this.config = config;
    this.configOriginals = config.originals();
    if (schemaRegistry == null) {
      List<String> urls = config.getSchemaRegistryUrls();
      int maxSchemaObject = config.getMaxSchemasPerSubject();
      Map<String, Object> originals = config.originalsWithPrefix("");
      schemaRegistry = SchemaRegistryClientFactory.newClient(
          urls,
          maxSchemaObject,
          Collections.singletonList(provider),
          originals,
          config.requestHeaders()
      );
    }

    contextNameStrategy = config.contextNameStrategy();
    keySubjectNameStrategy = config.keySubjectNameStrategy();
    keySchemaIdSerializer = config.keySchemaIdSerializer();
    keySchemaIdDeserializer = config.keySchemaIdDeserializer();
    valueSubjectNameStrategy = config.valueSubjectNameStrategy();
    valueSchemaIdSerializer = config.valueSchemaIdSerializer();
    valueSchemaIdDeserializer = config.valueSchemaIdDeserializer();
    useSchemaReflection = config.useSchemaReflection();
    useLatestVersion = config.useLatestVersion();
    int latestCacheSize = config.getLatestCacheSize();
    int latestCacheTtl = config.getLatestCacheTtl();
    CacheBuilder<Object, Object> latestVersionsBuilder = CacheBuilder.newBuilder()
        .maximumSize(latestCacheSize)
        .ticker(ticker);
    if (latestCacheTtl >= 0) {
      latestVersionsBuilder = latestVersionsBuilder.expireAfterWrite(
          latestCacheTtl, TimeUnit.SECONDS);
    }
    latestVersions = latestVersionsBuilder.build();
    CacheBuilder<Object, Object> latestWithMetadataBuilder = CacheBuilder.newBuilder()
        .maximumSize(latestCacheSize)
        .ticker(ticker);
    if (latestCacheTtl >= 0) {
      latestWithMetadataBuilder = latestWithMetadataBuilder.expireAfterWrite(
          latestCacheTtl, TimeUnit.SECONDS);
    }
    latestWithMetadata = latestWithMetadataBuilder.build();
    if (config.getLatestWithMetadataSpec() != null) {
      MapPropertyParser parser = new MapPropertyParser();
      metadata = parser.parse(config.getLatestWithMetadataSpec());
    }
    enableRuleServiceLoader = config.enableRuleServiceLoader();
    ruleExecutors = initRuleObjects(
        config, RULE_EXECUTORS, RuleExecutor.class, enableRuleServiceLoader);
    ruleActions = initRuleObjects(
        config, RULE_ACTIONS, RuleAction.class, enableRuleServiceLoader);
    onSuccessActions = new ConcurrentHashMap<>();
    onFailureActions = new ConcurrentHashMap<>();
    disabledFlags = new ConcurrentHashMap<>();
  }

  protected void postOp(Object payload) {
    if (isKey) {
      setKey(payload);
    } else {
      clearKey();
    }
  }

  private Map<String, Map<String, RuleBase>> initRuleObjects(
      AbstractKafkaSchemaSerDeConfig config, String configName,
      Class<? extends RuleBase> cls, boolean enableRuleServiceLoader) {
    List<String> names = config.getList(configName);
    Map<String, Map<String, RuleBase>> ruleObjects = names.stream()
        .flatMap(n -> initRuleObject(n, config, configName)
            .map(r -> new AbstractMap.SimpleEntry<>(n, r)))
        .collect(Collectors.groupingBy(e -> e.getValue().type(),
            Collectors.toMap(SimpleEntry::getKey, e -> {
              log.info("Registering rule object {} for {}: {}",
                  e.getKey(),
                  e.getValue().type(),
                  e.getValue().getClass().getName()
              );
              return e.getValue();
            }, (e1, e2) -> e1, LinkedHashMap::new)));
    if (enableRuleServiceLoader) {
      try {
        addRuleObjectsFromServiceLoader(ruleObjects, config, configName, cls,
            Thread.currentThread().getContextClassLoader());
      } catch (ServiceConfigurationError e) {
        // Try using the class loader of the given class
        addRuleObjectsFromServiceLoader(ruleObjects, config, configName, cls, cls.getClassLoader());
      }
    }
    return ruleObjects;
  }

  private Stream<RuleBase> initRuleObject(
      String name, AbstractKafkaSchemaSerDeConfig config, String configName) {
    String propertyName = configName + "." + name + ".class";
    Object propertyValue = configOriginals.get(propertyName);
    if (propertyValue == null) {
      return Stream.empty();
    }
    try {
      RuleBase ruleObject = propertyValue instanceof Class
          ? Utils.newInstance((Class<?>) propertyValue, RuleBase.class)
          : Utils.newInstance(propertyValue.toString(), RuleBase.class);
      configureRuleObject(ruleObject, name, config, configName);
      return Stream.of(ruleObject);
    } catch (ClassNotFoundException e) {
      log.error("Could not load rule object class {}", name, e);
      throw new ConfigException("Could not load rule object class " + name);
    }
  }

  private void addRuleObjectsFromServiceLoader(
      Map<String, Map<String, RuleBase>> ruleObjects,
      AbstractKafkaSchemaSerDeConfig config, String configName, Class<? extends RuleBase> cls,
      ClassLoader classLoader) {
    ServiceLoader<? extends RuleBase> serviceLoader = ServiceLoader.load(cls, classLoader);

    String name = RuleBase.DEFAULT_NAME;
    for (RuleBase ruleObject : serviceLoader) {
      configureRuleObject(ruleObject, name, config, configName);
      Map<String, RuleBase> rules = ruleObjects.computeIfAbsent(
          ruleObject.type(), k -> new LinkedHashMap<>());
      rules.put(name, ruleObject);
    }
  }

  private void configureRuleObject(
      RuleBase ruleObject, String name, AbstractKafkaSchemaSerDeConfig config, String configName) {
    Map<String, Object> params = new HashMap<>();
    if (ruleObject.addOriginalConfigs()) {
      params.putAll(configOriginals);
      // Don't propagate serializers
      params.remove(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
      params.remove(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
    } else {
      // copy props prefixed with "schema.registry."
      params.putAll(config.originalsWithPrefix(SchemaRegistryClientConfig.CLIENT_NAMESPACE, false));
    }
    // Add default params
    String prefix = configName + "." + RuleBase.DEFAULT_NAME + PARAM;
    params.putAll(config.originalsWithPrefix(prefix));
    // Add rule type specific params
    prefix = configName + "._" + ruleObject.type() + "_" + PARAM;
    params.putAll(config.originalsWithPrefix(prefix));
    // Add named params
    prefix = configName + "." + name + PARAM;
    params.putAll(config.originalsWithPrefix(prefix));
    ruleObject.configure(params);
  }

  public SchemaRegistryClient getSchemaRegistryClient() {
    return schemaRegistry;
  }

  // Visible for testing
  public Map<String, Map<String, RuleBase>> getRuleExecutors() {
    return ruleExecutors;
  }

  private RuleExecutor getRuleExecutor(RuleContext ctx) {
    return (RuleExecutor) getRuleObject(ctx, ruleExecutors, ctx.rule().getType());
  }

  // Visible for testing
  public Map<String, Map<String, RuleBase>> getRuleActions() {
    return ruleActions;
  }

  private RuleAction getRuleAction(RuleContext ctx, String actionName) {
    if (actionName.equals(ErrorAction.TYPE)) {
      return ERROR_ACTION;
    } else if (actionName.equals(NoneAction.TYPE)) {
      return NONE_ACTION;
    } else {
      return (RuleAction) getRuleObject(ctx, ruleActions, actionName);
    }
  }

  private RuleBase getRuleObject(
      RuleContext ctx, Map<String, Map<String, RuleBase>> ruleBases, String type) {
    Rule rule = ctx.rule();
    Map<String, RuleBase> ruleObjects = ruleBases.get(type.toUpperCase(Locale.ROOT));
    if (ruleObjects != null && !ruleObjects.isEmpty()) {
      // First try rule name qualified with subject
      RuleBase ruleObject = ruleObjects.get(ctx.subject() + ":" + rule.getName());
      if (ruleObject != null) {
        return ruleObject;
      }
      // Next try rule name
      ruleObject = ruleObjects.get(rule.getName());
      if (ruleObject != null) {
        return ruleObject;
      }
      // Finally try any rule object registered for the given rule type.
      // This is to allow multiple rules to use the same rule object.
      return ruleObjects.entrySet().iterator().next().getValue();
    }
    return null;
  }

  public boolean isKey() {
    return isKey;
  }

  protected Map<SubjectSchema, ExtendedSchema> latestVersionsCache() {
    return latestVersions != null ? latestVersions.asMap() : new HashMap<>();
  }

  protected Map<String, ExtendedSchema> latestWithMetadataCache() {
    return latestWithMetadata != null ? latestWithMetadata.asMap() : new HashMap<>();
  }

  protected ExtendedSchema getLatestWithMetadata(String subject)
      throws IOException, RestClientException {
    if (metadata == null || metadata.isEmpty()) {
      return null;
    }
    ExtendedSchema extendedSchema = latestWithMetadata.getIfPresent(subject);
    if (extendedSchema == null) {
      SchemaMetadata schemaMetadata = schemaRegistry.getLatestWithMetadata(subject, metadata, true);
      Optional<ParsedSchema> optSchema =
          schemaRegistry.parseSchema(
              new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(
                  null, schemaMetadata));
      ParsedSchema schema = optSchema.orElseThrow(
          () -> new IOException("Invalid schema " + schemaMetadata.getSchema()
              + " with refs " + schemaMetadata.getReferences()
              + " of type " + schemaMetadata.getSchemaType()));
      schema = schema.copy(schemaMetadata.getVersion());
      extendedSchema = new ExtendedSchema(
          schemaMetadata.getId(), schemaMetadata.getVersion(),
          schemaMetadata.getGuid(), schema
      );
      latestWithMetadata.put(subject, extendedSchema);
    }
    return extendedSchema;
  }

  private ParsedSchema getSchemaMetadata(String subject, int version)
      throws IOException, RestClientException {
    SchemaMetadata schemaMetadata = schemaRegistry.getSchemaMetadata(subject, version, true);
    Optional<ParsedSchema> optSchema =
        schemaRegistry.parseSchema(
            new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(
                null, schemaMetadata));
    ParsedSchema schema = optSchema.orElseThrow(
        () -> new IOException("Invalid schema " + schemaMetadata.getSchema()
            + " with refs " + schemaMetadata.getReferences()
            + " of type " + schemaMetadata.getSchemaType()));
    return schema.copy(schemaMetadata.getVersion());
  }

  protected List<Migration> getMigrations(
      String subject, ParsedSchema writerSchema, ParsedSchema readerSchema)
      throws IOException, RestClientException {
    RuleMode migrationMode = null;
    ParsedSchema first;
    ParsedSchema last;
    List<Migration> migrations = new ArrayList<>();
    if (writerSchema.version() < readerSchema.version()) {
      migrationMode = RuleMode.UPGRADE;
      first = writerSchema;
      last = readerSchema;
    } else if (writerSchema.version() > readerSchema.version()) {
      migrationMode = RuleMode.DOWNGRADE;
      first = readerSchema;
      last = writerSchema;
    } else {
      return migrations;
    }

    List<ParsedSchema> versions = getSchemasBetween(subject, first, last);
    ParsedSchema previous = null;
    for (int i = 0; i < versions.size(); i++) {
      ParsedSchema current = versions.get(i);
      if (i == 0) {
        // skip the first version
        previous = current;
        continue;
      }
      if (current.ruleSet() != null
          && current.ruleSet().hasRules(RulePhase.MIGRATION, migrationMode)) {
        Migration m;
        if (migrationMode == RuleMode.UPGRADE) {
          m = new Migration(migrationMode, previous, current);
        } else {
          m = new Migration(migrationMode, current, previous);
        }
        migrations.add(m);
      }
      previous = current;
    }
    if (migrationMode == RuleMode.DOWNGRADE) {
      Collections.reverse(migrations);
    }
    return migrations;
  }

  private List<ParsedSchema> getSchemasBetween(
      String subject, ParsedSchema first, ParsedSchema last)
      throws IOException, RestClientException {
    if (last.version() - first.version() <= 1) {
      return ImmutableList.of(first, last);
    }
    int version1 = first.version();
    int version2 = last.version();
    List<ParsedSchema> schemas = new ArrayList<>();
    schemas.add(first);
    for (int i = version1 + 1; i < version2; i++) {
      schemas.add(getSchemaMetadata(subject, i));
    }
    schemas.add(last);
    return schemas;
  }

  /**
   * Get the subject name for the given topic and value type.
   */
  protected String getSubjectName(String topic, boolean isKey, Object value, ParsedSchema schema) {
    SubjectNameStrategy subjectNameStrategy = subjectNameStrategy(isKey);
    String subject = subjectNameStrategy.subjectName(topic, isKey, schema);
    return getContextName(topic, subject);
  }

  protected String getContextName(String topic) {
    return getContextName(topic, null);
  }

  // Visible for testing
  protected String getContextName(String topic, String subject) {
    String contextName = contextNameStrategy.contextName(topic);
    if (contextName != null) {
      contextName = QualifiedSubject.normalizeContext(contextName);
      return subject != null ? contextName + subject : contextName;
    } else {
      return subject;
    }
  }

  protected boolean strategyUsesSchema(boolean isKey) {
    SubjectNameStrategy subjectNameStrategy = subjectNameStrategy(isKey);
    return subjectNameStrategy.usesSchema();
  }

  private SubjectNameStrategy subjectNameStrategy(boolean isKey) {
    return isKey ? keySubjectNameStrategy : valueSubjectNameStrategy;
  }

  protected SchemaIdSerializer schemaIdSerializer(boolean isKey) {
    return isKey ? keySchemaIdSerializer : valueSchemaIdSerializer;
  }

  protected SchemaIdDeserializer schemaIdDeserializer(boolean isKey) {
    return isKey ? keySchemaIdDeserializer : valueSchemaIdDeserializer;
  }

  /**
   * Get the subject name used by the old Encoder interface, which relies only on the value type
   * rather than the topic.
   */
  protected String getOldSubjectName(Object value) {
    if (value instanceof GenericContainer) {
      return ((GenericContainer) value).getSchema().getName() + "-value";
    } else {
      throw new SerializationException("Primitive types are not supported yet");
    }
  }

  @Deprecated
  public int register(String subject, org.apache.avro.Schema schema)
      throws IOException, RestClientException {
    return schemaRegistry.register(subject, schema);
  }

  public int register(String subject, ParsedSchema schema) throws IOException, RestClientException {
    return schemaRegistry.register(subject, schema);
  }

  public int register(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    return schemaRegistry.register(subject, schema, normalize);
  }

  protected Schema registerWithResponse(String subject, ParsedSchema schema,
      boolean normalize, boolean propagateSchemaTags)
      throws IOException, RestClientException {
    RegisterSchemaResponse response =
        schemaRegistry.registerWithResponse(subject, schema, normalize, propagateSchemaTags);
    return new Schema(subject, response);
  }

  @Deprecated
  public org.apache.avro.Schema getById(int id) throws IOException, RestClientException {
    return schemaRegistry.getById(id);
  }

  public ParsedSchema getSchemaById(int id) throws IOException, RestClientException {
    return schemaRegistry.getSchemaById(id);
  }

  @Deprecated
  public org.apache.avro.Schema getBySubjectAndId(String subject, int id)
      throws IOException, RestClientException {
    return schemaRegistry.getBySubjectAndId(subject, id);
  }

  public ParsedSchema getSchemaBySubjectAndId(String subject, int id)
      throws IOException, RestClientException {
    return schemaRegistry.getSchemaBySubjectAndId(subject, id);
  }

  protected ParsedSchema getSchemaBySchemaId(String subject, SchemaId schemaId)
      throws IOException, RestClientException {
    if (schemaId.getId() != null) {
      return schemaRegistry.getSchemaBySubjectAndId(subject, schemaId.getId());
    } else if (schemaId.getGuid() != null) {
      return schemaRegistry.getSchemaByGuid(schemaId.getGuid().toString(), null);
    } else {
      throw new SerializationException("Could not deserialize schema ID");
    }
  }

  protected ParsedSchema lookupSchemaBySubjectAndId(
      String subject, int id, ParsedSchema schema, boolean idCompatStrict)
      throws IOException, RestClientException {
    ParsedSchema lookupSchema = getSchemaBySubjectAndId(subject, id);
    if (idCompatStrict && !lookupSchema.isBackwardCompatible(schema).isEmpty()) {
      throw new IOException("Incompatible schema '" + lookupSchema.canonicalString()
          + "' with refs '" + lookupSchema.references()
          + "' of type '" + lookupSchema.schemaType()
          + "' for schema '" + schema.canonicalString()
          + "'. Set id.compatibility.strict=false to disable this check");
    }
    return lookupSchema;
  }

  protected ExtendedSchema lookupLatestVersion(
      String subject, ParsedSchema schema, boolean latestCompatStrict)
      throws IOException, RestClientException {
    return lookupLatestVersion(
        schemaRegistry, subject, schema, latestVersionsCache(), latestCompatStrict);
  }

  protected static ExtendedSchema lookupLatestVersion(
      SchemaRegistryClient schemaRegistry,
      String subject,
      ParsedSchema schema,
      Map<SubjectSchema, ExtendedSchema> cache,
      boolean latestCompatStrict)
      throws IOException, RestClientException {
    SubjectSchema ss = new SubjectSchema(subject, schema);
    ExtendedSchema extendedSchema = null;
    if (cache != null) {
      extendedSchema = cache.get(ss);
    }
    if (extendedSchema == null) {
      SchemaMetadata schemaMetadata = schemaRegistry.getLatestSchemaMetadata(subject);
      Optional<ParsedSchema> optSchema =
          schemaRegistry.parseSchema(
              new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(
                  null, schemaMetadata));
      ParsedSchema latestVersion = optSchema.orElseThrow(
          () -> new IOException("Invalid schema " + schemaMetadata.getSchema()
              + " with refs " + schemaMetadata.getReferences()
              + " of type " + schemaMetadata.getSchemaType()));
      latestVersion = latestVersion.copy(schemaMetadata.getVersion());
      // Sanity check by testing latest is backward compatibility with schema
      // Don't test for forward compatibility so unions can be handled properly
      if (latestCompatStrict) {
        List<String> errorMessages = latestVersion.isBackwardCompatible(schema);
        if (!errorMessages.isEmpty()) {
          String baseMsg = "Incompatible schema '" + schemaMetadata.getSchema()
                  + "' with refs '" + schemaMetadata.getReferences()
                  + "' of type '" + schemaMetadata.getSchemaType()
                  + "' for schema '" + schema.canonicalString()
                  + "'. Set latest.compatibility.strict=false to disable this check.";
          log.error(baseMsg + " Error messages: " + String.join(",", errorMessages)
                  + "; latestVersion=" + latestVersion + "; schema=" + schema);
          throw new IOException(baseMsg + " See log file for more details.");
        }
      }
      extendedSchema = new ExtendedSchema(
          schemaMetadata.getId(), schemaMetadata.getVersion(),
          schemaMetadata.getGuid(), latestVersion
      );
      if (cache != null) {
        cache.put(ss, extendedSchema);
      }
    }
    return extendedSchema;
  }

  protected Object executeMigrations(
      List<Migration> migrations, String subject, String topic,
      Headers headers, Object message) throws IOException {
    for (int i = 0; i < migrations.size(); i++) {
      Migration m = migrations.get(i);
      if (i == 0) {
        // Migrations use JsonNode for both input and output
        message = m.getSource().toJson(message);
      }
      message = executeRules(
          subject, topic, headers, message, RulePhase.MIGRATION, m.getRuleMode(),
          m.getSource(), m.getTarget(), message
      );
    }
    return message;
  }

  protected Object executeRules(
      String subject, String topic, Headers headers,
      RuleMode ruleMode, ParsedSchema source, ParsedSchema target, Object message) {
    return executeRules(subject, topic, headers, message, ruleMode, source, target, message);
  }

  protected Object executeRules(
      String subject, String topic, Headers headers, Object original,
      RuleMode ruleMode, ParsedSchema source, ParsedSchema target, Object message) {
    return executeRules(
        subject, topic, headers, original, RulePhase.DOMAIN, ruleMode, source, target, message);
  }

  protected Object executeRules(
      String subject, String topic, Headers headers, Object original,
      RulePhase rulePhase, RuleMode ruleMode,
      ParsedSchema source, ParsedSchema target, Object message) {
    if (message == null || target == null) {
      return message;
    }
    List<Rule> rules = Collections.emptyList();
    if (ruleMode == RuleMode.UPGRADE) {
      if (target.ruleSet() != null) {
        rules = target.ruleSet().getMigrationRules();
      }
    } else if (ruleMode == RuleMode.DOWNGRADE) {
      if (source.ruleSet() != null) {
        rules = new ArrayList<>(source.ruleSet().getMigrationRules());
        // Execute downgrade rules in reverse order for symmetry
        Collections.reverse(rules);
      }
    } else {
      if (target.ruleSet() != null) {
        rules = rulePhase == RulePhase.ENCODING
            ? target.ruleSet().getEncodingRules()
            : target.ruleSet().getDomainRules();
        if (ruleMode == RuleMode.READ) {
          rules = new ArrayList<>(rules);
          // Execute read rules in reverse order for symmetry
          Collections.reverse(rules);
        }
      }
    }
    for (int i = 0; i < rules.size(); i++) {
      Rule rule = rules.get(i);
      if (skipRule(rule, headers)) {
        continue;
      }
      if (rule.getMode() == RuleMode.WRITEREAD) {
        if (ruleMode != RuleMode.READ && ruleMode != RuleMode.WRITE) {
          continue;
        }
      } else if (rule.getMode() == RuleMode.UPDOWN) {
        if (ruleMode != RuleMode.UPGRADE && ruleMode != RuleMode.DOWNGRADE) {
          continue;
        }
      } else if (ruleMode != rule.getMode()) {
        continue;
      }
      RuleContext ctx = new RuleContext(configOriginals, source, target,
          subject, topic, headers,
          isKey ? original : key(),
          isKey ? null : original,
          isKey, ruleMode, rule, i, rules);
      RuleExecutor ruleExecutor = getRuleExecutor(ctx);
      if (ruleExecutor != null) {
        try {
          Object result = ruleExecutor.transform(ctx, message);
          switch (rule.getKind()) {
            case CONDITION:
              if (Boolean.FALSE.equals(result)) {
                throw new RuleConditionException(rule);
              }
              break;
            case TRANSFORM:
              message = result;
              break;
            default:
              throw new IllegalStateException("Unsupported rule kind " + rule.getKind());
          }
          runAction(ctx, ruleMode, rule,
              message != null ? getOnSuccess(rule) : getOnFailure(rule),
              message, null, message != null ? null : ErrorAction.TYPE
          );
        } catch (RuleException e) {
          runAction(ctx, ruleMode, rule, getOnFailure(rule), message, e, ErrorAction.TYPE);
        }
      } else {
        runAction(ctx, ruleMode, rule, getOnFailure(rule), message,
            new RuleException("Could not find rule executor of type " + rule.getType()),
            ErrorAction.TYPE);
      }
    }
    return message;
  }

  private String getOnSuccess(Rule rule) {
    return onSuccessActions.computeIfAbsent(rule, r -> {
      Object propertyValue = getRuleConfig(rule.getName(), ON_SUCCESS);
      if (propertyValue != null) {
        return propertyValue.toString();
      }
      propertyValue = getRuleConfig("_" + rule.getType() + "_", ON_SUCCESS);
      if (propertyValue != null) {
        return propertyValue.toString();
      }
      propertyValue = getRuleConfig(RuleBase.DEFAULT_NAME, ON_SUCCESS);
      if (propertyValue != null) {
        return propertyValue.toString();
      }
      return rule.getOnSuccess();
    });
  }

  private String getOnFailure(Rule rule) {
    return onFailureActions.computeIfAbsent(rule, r -> {
      Object propertyValue = getRuleConfig(rule.getName(), ON_FAILURE);
      if (propertyValue != null) {
        return propertyValue.toString();
      }
      propertyValue = getRuleConfig("_" + rule.getType() + "_", ON_FAILURE);
      if (propertyValue != null) {
        return propertyValue.toString();
      }
      propertyValue = getRuleConfig(RuleBase.DEFAULT_NAME, ON_FAILURE);
      if (propertyValue != null) {
        return propertyValue.toString();
      }
      return rule.getOnFailure();
    });
  }

  private boolean isDisabled(Rule rule) {
    return disabledFlags.computeIfAbsent(rule, r -> {
      Object propertyValue = getRuleConfig(rule.getName(), DISABLED);
      if (propertyValue != null) {
        return Boolean.parseBoolean(propertyValue.toString());
      }
      propertyValue = getRuleConfig("_" + rule.getType() + "_", DISABLED);
      if (propertyValue != null) {
        return Boolean.parseBoolean(propertyValue.toString());
      }
      propertyValue = getRuleConfig(RuleBase.DEFAULT_NAME, DISABLED);
      if (propertyValue != null) {
        return Boolean.parseBoolean(propertyValue.toString());
      }
      return rule.isDisabled();
    });
  }

  private Object getRuleConfig(String name, String suffix) {
    String propertyName = RULE_EXECUTORS + "." + name + "." + suffix;
    return configOriginals.get(propertyName);
  }

  private boolean skipRule(Rule rule, Headers headers) {
    if (isDisabled(rule)) {
      return true;
    }
    if (headers != null) {
      Header header = headers.lastHeader(DlqAction.RULE_NAME);
      if (header != null) {
        String ruleName = new String(header.value(), StandardCharsets.UTF_8);
        if (rule.getName().equals(ruleName)) {
          // If the rule name exists as a header, then we are deserializing from a DLQ.
          // In that case, we don't want deserialization to fail again,
          // so we ignore the rule that previously failed.
          return true;
        }
      }
    }
    return false;
  }

  private void runAction(RuleContext ctx, RuleMode ruleMode, Rule rule, String action,
      Object message, RuleException ex, String defaultAction) {
    String actionName = getRuleActionName(rule, ruleMode, action);
    if (actionName == null) {
      actionName = defaultAction;
    }
    if (actionName != null) {
      RuleAction ruleAction = getRuleAction(ctx, actionName);
      if (ruleAction == null) {
        log.error("Could not find rule action of type {}", actionName);
        throw new ConfigException("Could not find rule action of type " + actionName);
      }
      try {
        ruleAction.run(ctx, message, ex);
      } catch (RuleException e) {
        log.error("Could not run post-rule action {}", action, e);
      }
    }
  }

  private String getRuleActionName(Rule rule, RuleMode ruleMode, String actionName) {
    if ((rule.getMode() == RuleMode.WRITEREAD || rule.getMode() == RuleMode.UPDOWN)
        && actionName != null
        && actionName.contains(",")) {
      String[] parts = actionName.split(",");
      switch (ruleMode) {
        case WRITE:
        case UPGRADE:
          return parts[0];
        case READ:
        case DOWNGRADE:
          return parts[1];
        default:
          throw new IllegalStateException("Unsupported rule mode " + ruleMode);
      }
    } else {
      return actionName;
    }
  }

  @Override
  public void close() throws IOException {
    closeRuleObjects(ruleActions);
    closeRuleObjects(ruleExecutors);
    if (schemaRegistry != null) {
      schemaRegistry.close();
    }
  }

  private void closeRuleObjects(Map<String, Map<String, RuleBase>> ruleBases) {
    if (ruleBases != null) {
      for (Map.Entry<String, Map<String, RuleBase>> outer : ruleBases.entrySet()) {
        for (Map.Entry<String, RuleBase> inner : outer.getValue().entrySet()) {
          closeQuietly(
              inner.getValue(), "rule object " + inner.getKey() + " for " + outer.getKey());
        }
      }
    }

  }

  private static void closeQuietly(AutoCloseable closeable, String name) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (Throwable t) {
        log.error("Failed to close {} with type {}", name, closeable.getClass().getName(), t);
      }
    }
  }

  protected static KafkaException toKafkaException(RestClientException e, String errorMessage) {
    int status = e.getStatus();
    if (status == 401) {
      return new AuthenticationException(errorMessage, e);
    } else if (status == 403) {
      return new AuthorizationException(errorMessage, e);
    } else if (status == 429) {        // Too Many Requests
      return new ThrottlingQuotaExceededException(e.getMessage());
    } else if (status == 408    // Request Timeout
        || status == 500        // Internal Server Error (includes timeouts and forwarding errors)
        || status == 503        // Service Unavailable
        || status == 504) {     // Gateway Timeout
      return new TimeoutException(errorMessage, e);
    } else if (status == 502) { // Bad Gateway
      return new DisconnectException(errorMessage, e);
    } else {
      return new SerializationException(errorMessage, e);
    }
  }

  protected static class SubjectSchema {
    private final String subject;
    private final ParsedSchema schema;

    public SubjectSchema(String subject, ParsedSchema schema) {
      this.subject = subject;
      this.schema = schema;
    }

    public String getSubject() {
      return subject;
    }

    public ParsedSchema getSchema() {
      return schema;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SubjectSchema that = (SubjectSchema) o;
      return subject.equals(that.subject)
          && schema.equals(that.schema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(subject, schema);
    }
  }

  protected static class ExtendedSchema {
    private final Integer id;
    private final Integer version;
    private final String guid;
    private final ParsedSchema schema;

    public ExtendedSchema(Integer id, Integer version, String guid, ParsedSchema schema) {
      this.id = id;
      this.version = version;
      this.guid = guid;
      this.schema = schema;
    }

    public Integer getId() {
      return id;
    }

    public Integer getVersion() {
      return version;
    }

    public String getGuid() {
      return guid;
    }

    public ParsedSchema getSchema() {
      return schema;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ExtendedSchema that = (ExtendedSchema) o;
      return id.equals(that.id)
          && version.equals(that.version)
          && Objects.equals(guid, that.guid)
          && schema.equals(that.schema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, version, guid, schema);
    }
  }

  protected static class Migration {
    private final RuleMode ruleMode;
    private final ParsedSchema source;
    private final ParsedSchema target;

    public Migration(RuleMode ruleMode, ParsedSchema source, ParsedSchema target) {
      this.ruleMode = ruleMode;
      this.source = source;
      this.target = target;
    }

    public RuleMode getRuleMode() {
      return ruleMode;
    }

    public ParsedSchema getSource() {
      return source;
    }

    public ParsedSchema getTarget() {
      return target;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Migration migration = (Migration) o;
      return ruleMode == migration.ruleMode
          && Objects.equals(source, migration.source)
          && Objects.equals(target, migration.target);
    }

    @Override
    public int hashCode() {
      return Objects.hash(ruleMode, source, target);
    }
  }

  static class ListPropertyParser {
    private static final char DELIM_CHAR = ',';
    private static final char QUOTE_CHAR = '\'';

    private final CsvMapper mapper;
    private final CsvSchema schema;

    public ListPropertyParser() {
      mapper = new CsvMapper()
          .enable(CsvGenerator.Feature.STRICT_CHECK_FOR_QUOTING)
          .enable(CsvParser.Feature.WRAP_AS_ARRAY);
      schema = CsvSchema.builder()
          .setColumnSeparator(DELIM_CHAR)
          .setQuoteChar(QUOTE_CHAR)
          .setLineSeparator("")
          .build();
    }

    public List<String> parse(String str) {
      try {
        ObjectReader reader = mapper.readerFor(String[].class).with(schema);
        Iterator<String[]> iter = reader.readValues(str);
        String[] strings = iter.hasNext() ? iter.next() : new String[0];
        return Arrays.asList(strings);
      } catch (IOException e) {
        throw new IllegalArgumentException("Could not parse string " + str, e);
      }
    }

    public String asString(List<String> list) {
      try {
        String[] array = list.toArray(new String[0]);
        ObjectWriter writer = mapper.writerFor(Object[].class).with(schema);
        return writer.writeValueAsString(array);
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("Could not parse list " + list, e);
      }
    }
  }

  static class MapPropertyParser {
    private final ListPropertyParser parser;

    public MapPropertyParser() {
      parser = new ListPropertyParser();
    }

    public Map<String, String> parse(String str) {
      List<String> strings = parser.parse(str);
      return strings.stream()
          .collect(Collectors.toMap(
              s -> s.substring(0, s.indexOf('=')),
              s -> s.substring(s.indexOf('=') + 1))
          );
    }

    public String asString(Map<String, String> map) {
      List<String> entries = map.entrySet().stream()
          .map(e -> e.getKey() + "=" + e.getValue())
          .collect(Collectors.toList());
      return parser.asString(entries);
    }
  }
}
