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

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.schemaregistry.rules.RuleExecutor;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;

import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.confluent.kafka.serializers.context.NullContextNameStrategy;
import io.confluent.kafka.serializers.context.strategy.ContextNameStrategy;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common fields and helper methods for both the serializer and the deserializer.
 */
public abstract class AbstractKafkaSchemaSerDe {

  private static final Logger log = LoggerFactory.getLogger(AbstractKafkaSchemaSerDe.class);

  protected static final byte MAGIC_BYTE = 0x0;
  protected static final int idSize = 4;
  protected static final int DEFAULT_CACHE_CAPACITY = 1000;

  protected SchemaRegistryClient schemaRegistry;
  protected ContextNameStrategy contextNameStrategy = new NullContextNameStrategy();
  protected Object keySubjectNameStrategy = new TopicNameStrategy();
  protected Object valueSubjectNameStrategy = new TopicNameStrategy();
  protected Map<SubjectSchema, ParsedSchema> latestVersions =
      new BoundedConcurrentHashMap<>(DEFAULT_CACHE_CAPACITY);
  protected Map<String, ParsedSchema> latestWithMetadata =
      new BoundedConcurrentHashMap<>(DEFAULT_CACHE_CAPACITY);
  protected boolean useSchemaReflection;
  protected boolean useLatestVersion;
  protected Map<String, String> metadata;
  protected Map<String, RuleExecutor> ruleExecutors;
  protected boolean isKey;

  protected void configureClientProperties(
      AbstractKafkaSchemaSerDeConfig config,
      SchemaProvider provider) {
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
    valueSubjectNameStrategy = config.valueSubjectNameStrategy();
    useSchemaReflection = config.useSchemaReflection();
    useLatestVersion = config.useLatestVersion();
    if (config.getLatestWithMetadataSpec() != null) {
      MapPropertyParser parser = new MapPropertyParser();
      metadata = parser.parse(config.getLatestWithMetadataSpec());
    }
    ruleExecutors = initRuleExecutors(config);
  }

  private Map<String, RuleExecutor> initRuleExecutors(
      AbstractKafkaSchemaSerDeConfig config) {
    List<String> names = config.getList(RULE_EXECUTORS);
    return names.stream()
        .flatMap(n -> initRuleExecutor(n, config))
        .collect(Collectors.toMap(RuleExecutor::type, e -> {
          log.info("Registering rule executor for {}: {}",
              e.type(),
              e.getClass().getName()
          );
          return e;
        }));
  }

  private Stream<RuleExecutor> initRuleExecutor(
      String name, AbstractKafkaSchemaSerDeConfig config) {
    String propertyName = RULE_EXECUTORS + "." + name + ".class";
    Object propertyValue = config.originals().get(propertyName);
    if (propertyValue == null) {
      return Stream.empty();
    }
    String className = propertyValue.toString();
    String prefix = RULE_EXECUTORS + "." + name + ".param.";
    Map<String, Object> params = config.originalsWithPrefix(prefix);
    try {
      RuleExecutor ruleExecutor = Utils.newInstance(className, RuleExecutor.class);
      ruleExecutor.configure(params);
      return Stream.of(ruleExecutor);
    } catch (ClassNotFoundException e) {
      log.error("Could not load rule executor class " + name, e);
      throw new ConfigException("Could not load rule executor class " + name);
    }
  }

  public boolean isKey() {
    return isKey;
  }

  protected ParsedSchema getLatestWithMetadata(String subject)
      throws IOException, RestClientException {
    if (metadata == null || metadata.isEmpty()) {
      return null;
    }
    ParsedSchema schema = latestWithMetadata.get(subject);
    if (schema == null) {
      SchemaMetadata schemaMetadata = schemaRegistry.getLatestWithMetadata(subject, metadata, true);
      Optional<ParsedSchema> optSchema =
          schemaRegistry.parseSchema(
              new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(
                  null, schemaMetadata));
      schema = optSchema.orElseThrow(
          () -> new IOException("Invalid schema " + schemaMetadata.getSchema()
              + " with refs " + schemaMetadata.getReferences()
              + " of type " + schemaMetadata.getSchemaType()));
      schema = schema.copy(schemaMetadata.getVersion());
      latestWithMetadata.put(subject, schema);
    }
    return schema;
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
      if (current.ruleSet().hasRules(migrationMode)) {
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
    for (int i = version1 + 1; i < version2; i++) {
      schemas.add(getSchemaMetadata(subject, i));
    }
    List<ParsedSchema> result = new ArrayList<>();
    result.add(first);
    result.addAll(schemas);
    result.add(last);
    return result;
  }

  /**
   * Get the subject name for the given topic and value type.
   */
  protected String getSubjectName(String topic, boolean isKey, Object value, ParsedSchema schema) {
    Object subjectNameStrategy = subjectNameStrategy(isKey);
    String subject;
    if (subjectNameStrategy instanceof SubjectNameStrategy) {
      subject = ((SubjectNameStrategy) subjectNameStrategy).subjectName(topic, isKey, schema);
    } else {
      subject = ((io.confluent.kafka.serializers.subject.SubjectNameStrategy) subjectNameStrategy)
          .getSubjectName(topic, isKey, value);
    }
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
    Object subjectNameStrategy = subjectNameStrategy(isKey);
    if (subjectNameStrategy instanceof SubjectNameStrategy) {
      return ((SubjectNameStrategy) subjectNameStrategy).usesSchema();
    } else {
      return false;
    }
  }

  protected boolean isDeprecatedSubjectNameStrategy(boolean isKey) {
    Object subjectNameStrategy = subjectNameStrategy(isKey);
    return !(
        subjectNameStrategy
            instanceof io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy);
  }

  private Object subjectNameStrategy(boolean isKey) {
    return isKey ? keySubjectNameStrategy : valueSubjectNameStrategy;
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
  public int register(String subject, Schema schema) throws IOException, RestClientException {
    return schemaRegistry.register(subject, schema);
  }

  public int register(String subject, ParsedSchema schema) throws IOException, RestClientException {
    return schemaRegistry.register(subject, schema);
  }

  public int register(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    return schemaRegistry.register(subject, schema, normalize);
  }

  public int register(String subject, ParsedSchema schema, boolean normalize, boolean verbose)
      throws IOException, RestClientException {
    return schemaRegistry.register(subject, schema, normalize, verbose);
  }

  @Deprecated
  public Schema getById(int id) throws IOException, RestClientException {
    return schemaRegistry.getById(id);
  }

  public ParsedSchema getSchemaById(int id) throws IOException, RestClientException {
    return schemaRegistry.getSchemaById(id);
  }

  @Deprecated
  public Schema getBySubjectAndId(String subject, int id)
      throws IOException, RestClientException {
    return schemaRegistry.getBySubjectAndId(subject, id);
  }

  public ParsedSchema getSchemaBySubjectAndId(String subject, int id)
      throws IOException, RestClientException {
    return schemaRegistry.getSchemaBySubjectAndId(subject, id);
  }

  protected ParsedSchema lookupSchemaBySubjectAndId(
      String subject, int id, ParsedSchema schema, boolean idCompatStrict)
      throws IOException, RestClientException {
    ParsedSchema lookupSchema = getSchemaBySubjectAndId(subject, id);
    if (idCompatStrict && !lookupSchema.isBackwardCompatible(schema).isEmpty()) {
      throw new IOException("Incompatible schema " + lookupSchema.canonicalString()
          + " with refs " + lookupSchema.references()
          + " of type " + lookupSchema.schemaType()
          + " for schema " + schema.canonicalString()
          + ". Set id.compatibility.strict=false to disable this check");
    }
    return lookupSchema;
  }

  protected ParsedSchema lookupLatestVersion(
      String subject, ParsedSchema schema, boolean latestCompatStrict)
      throws IOException, RestClientException {
    return lookupLatestVersion(schemaRegistry, subject, schema, latestVersions, latestCompatStrict);
  }

  protected static ParsedSchema lookupLatestVersion(
      SchemaRegistryClient schemaRegistry,
      String subject,
      ParsedSchema schema,
      Map<SubjectSchema, ParsedSchema> cache,
      boolean latestCompatStrict)
      throws IOException, RestClientException {
    SubjectSchema ss = new SubjectSchema(subject, schema);
    ParsedSchema latestVersion = null;
    if (cache != null) {
      latestVersion = cache.get(ss);
    }
    if (latestVersion == null) {
      SchemaMetadata schemaMetadata = schemaRegistry.getLatestSchemaMetadata(subject);
      Optional<ParsedSchema> optSchema =
          schemaRegistry.parseSchema(
              new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(
                  null, schemaMetadata));
      latestVersion = optSchema.orElseThrow(
          () -> new IOException("Invalid schema " + schemaMetadata.getSchema()
              + " with refs " + schemaMetadata.getReferences()
              + " of type " + schemaMetadata.getSchemaType()));
      latestVersion = latestVersion.copy(schemaMetadata.getVersion());
      // Sanity check by testing latest is backward compatibility with schema
      // Don't test for forward compatibility so unions can be handled properly
      if (latestCompatStrict && !latestVersion.isBackwardCompatible(schema).isEmpty()) {
        throw new IOException("Incompatible schema " + schemaMetadata.getSchema()
            + " with refs " + schemaMetadata.getReferences()
            + " of type " + schemaMetadata.getSchemaType()
            + " for schema " + schema.canonicalString()
            + ". Set latest.compatibility.strict=false to disable this check");
      }
      if (cache != null) {
        cache.put(ss, latestVersion);
      }
    }
    return latestVersion;
  }

  protected ByteBuffer getByteBuffer(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    if (buffer.get() != MAGIC_BYTE) {
      throw new SerializationException("Unknown magic byte!");
    }
    return buffer;
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
          subject, topic, headers, m.getRuleMode(),
          m.getSource(), m.getTarget(), message
      );
    }
    return message;
  }

  protected Object executeRules(
      String subject, String topic, Headers headers,
      RuleMode ruleMode, ParsedSchema source, ParsedSchema target, Object message) {
    if (message == null || target == null) {
      return message;
    }
    List<Rule> rules;
    if (ruleMode == RuleMode.UPGRADE) {
      rules = target.ruleSet().getMigrationRules();
    } else if (ruleMode == RuleMode.DOWNGRADE) {
      rules = source.ruleSet().getMigrationRules();
    } else {
      rules = target.ruleSet().getDomainRules();
    }
    for (Rule rule : rules) {
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
      RuleContext ctx = new RuleContext(source, target,
          subject, topic, headers, isKey, ruleMode, rule);
      RuleExecutor ruleExecutor = ruleExecutors.get(rule.getType());
      if (ruleExecutor == null) {
        log.warn("Could not find rule executor of type {}", rule.getType());
        return message;
      }
      try {
        message = ruleExecutor.transform(ctx, message);
        if (message == null) {
          throw new SerializationException("Validation failed for rule " + rule);
        }
      } catch (RuleException e) {
        throw new SerializationException("Could not execute rule " + rule, e);
      }
    }
    return message;
  }

  protected static KafkaException toKafkaException(RestClientException e, String errorMessage) {
    if (e.getErrorCode() / 100 == 4 /* Client Error */) {
      return new InvalidConfigurationException(e.getMessage());
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
