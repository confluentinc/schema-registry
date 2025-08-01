/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.json;

import static io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity.EntityType.SR_FIELD;
import static io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity.EntityType.SR_RECORD;
import static io.confluent.kafka.schemaregistry.json.JsonSchemaUtils.findMatchingEntity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.BeanDeserializer;
import com.fasterxml.jackson.databind.deser.DefaultDeserializationContext.Impl;
import com.fasterxml.jackson.databind.deser.SettableBeanProperty;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.PropertyWriter;
import com.github.erosb.jsonsKema.JsonArray;
import com.github.erosb.jsonsKema.JsonBoolean;
import com.github.erosb.jsonsKema.JsonNull;
import com.github.erosb.jsonsKema.JsonNumber;
import com.github.erosb.jsonsKema.JsonObject;
import com.github.erosb.jsonsKema.JsonString;
import com.github.erosb.jsonsKema.JsonValue;
import com.github.erosb.jsonsKema.SchemaLoaderConfig;
import com.github.erosb.jsonsKema.UnknownSource;
import com.github.erosb.jsonsKema.ValidationFailure;
import com.github.erosb.jsonsKema.Validator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.diff.Difference;
import io.confluent.kafka.schemaregistry.json.diff.SchemaDiff;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import io.confluent.kafka.schemaregistry.json.schema.SchemaTranslator;
import io.confluent.kafka.schemaregistry.rules.FieldTransform;
import io.confluent.kafka.schemaregistry.rules.RuleConditionException;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleContext.FieldContext;
import io.confluent.kafka.schemaregistry.rules.RuleContext.Type;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ConditionalSchema;
import org.everit.json.schema.ConstSchema;
import org.everit.json.schema.EmptySchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.FalseSchema;
import org.everit.json.schema.NotSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;
import org.everit.json.schema.TrueSchema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.everit.json.schema.loader.internal.ReferenceResolver;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonSchema implements ParsedSchema {

  private static final Logger log = LoggerFactory.getLogger(JsonSchema.class);

  public static final String DEFAULT_BASE_URI = "mem://input";

  public static final String TYPE = "JSON";

  public static final String TAGS = "confluent:tags";

  private static final String SCHEMA_KEYWORD = "$schema";

  private static final String PROPERTIES_KEYWORD = "properties";

  private static final Object NONE_MARKER = new Object();

  private final JsonNode jsonNode;

  private transient Schema schemaObj;

  private transient com.github.erosb.jsonsKema.Schema skemaObj;

  private final Integer version;

  private final List<SchemaReference> references;

  private final Map<String, String> resolvedReferences;

  private final Metadata metadata;

  private final RuleSet ruleSet;

  private final boolean ignoreModernDialects;

  private transient String canonicalString;

  private transient int hashCode = NO_HASHCODE;

  private static final int NO_HASHCODE = Integer.MIN_VALUE;
  private static final int DEFAULT_CACHE_CAPACITY = 1000;

  private static final ObjectMapper objectMapper = Jackson.newObjectMapper();
  private static final ObjectMapper objectMapperWithOrderedProps = Jackson.newObjectMapper(true);

  private static final Map<String, Map<String, BeanPropertyWriter>> beanGetters =
      new BoundedConcurrentHashMap<>(DEFAULT_CACHE_CAPACITY);
  private static final Map<String, Map<String, SettableBeanProperty>> beanSetters =
      new BoundedConcurrentHashMap<>(DEFAULT_CACHE_CAPACITY);

  // prepopulate the draft 2019-09 metaschemas, as the json-sKema library
  // only prepopulates the draft 2020-12 metaschemas
  private static final Map<URI, String> prepopulatedMetaSchemas = ImmutableMap.of(
      URI.create("https://json-schema.org/draft/2019-09/schema"),
      readFromClassPath("/metaschemas/draft/2019-09/schema"),
      URI.create("https://json-schema.org/draft/2019-09/meta/core"),
      readFromClassPath("/metaschemas/draft/2019-09/meta/core"),
      URI.create("https://json-schema.org/draft/2019-09/meta/validation"),
      readFromClassPath("/metaschemas/draft/2019-09/meta/validation"),
      URI.create("https://json-schema.org/draft/2019-09/meta/applicator"),
      readFromClassPath("/metaschemas/draft/2019-09/meta/applicator"),
      URI.create("https://json-schema.org/draft/2019-09/meta/meta-data"),
      readFromClassPath("/metaschemas/draft/2019-09/meta/meta-data"),
      URI.create("https://json-schema.org/draft/2019-09/meta/format"),
      readFromClassPath("/metaschemas/draft/2019-09/meta/format"),
      URI.create("https://json-schema.org/draft/2019-09/meta/content"),
      readFromClassPath("/metaschemas/draft/2019-09/meta/content")
  );

  public JsonSchema(JsonNode jsonNode) {
    this(jsonNode, Collections.emptyList(), Collections.emptyMap(), null);
  }

  public JsonSchema(String schemaString) {
    this(schemaString, Collections.emptyList(), Collections.emptyMap(), null);
  }

  public JsonSchema(
      JsonNode jsonNode,
      List<SchemaReference> references,
      Map<String, String> resolvedReferences,
      Integer version
  ) {
    this.jsonNode = jsonNode;
    this.version = version;
    this.references = Collections.unmodifiableList(references);
    this.resolvedReferences = Collections.unmodifiableMap(resolvedReferences);
    this.metadata = null;
    this.ruleSet = null;
    this.ignoreModernDialects = false;
  }

  public JsonSchema(
      String schemaString,
      List<SchemaReference> references,
      Map<String, String> resolvedReferences,
      Integer version
  ) {
    this(schemaString, references, resolvedReferences, null, null, version);
  }

  public JsonSchema(
      String schemaString,
      List<SchemaReference> references,
      Map<String, String> resolvedReferences,
      Metadata metadata,
      RuleSet ruleSet,
      Integer version
  ) {
    try {
      this.jsonNode = schemaString != null ? objectMapper.readTree(schemaString) : null;
      this.version = version;
      this.references = Collections.unmodifiableList(references);
      this.resolvedReferences = Collections.unmodifiableMap(resolvedReferences);
      this.metadata = metadata;
      this.ruleSet = ruleSet;
      this.ignoreModernDialects = false;
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid JSON " + schemaString, e);
    }
  }

  public JsonSchema(Schema schemaObj) {
    this(schemaObj, null);
  }

  public JsonSchema(Schema schemaObj, Integer version) {
    try {
      this.jsonNode = schemaObj != null ? objectMapper.readTree(schemaObj.toString()) : null;
      this.schemaObj = schemaObj;
      this.version = version;
      this.references = Collections.emptyList();
      this.resolvedReferences = Collections.emptyMap();
      this.metadata = null;
      this.ruleSet = null;
      this.ignoreModernDialects = false;
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid JSON " + schemaObj, e);
    }
  }

  private JsonSchema(
      JsonNode jsonNode,
      com.github.erosb.jsonsKema.Schema skemaObj,
      Schema schemaObj,
      Integer version,
      List<SchemaReference> references,
      Map<String, String> resolvedReferences,
      Metadata metadata,
      RuleSet ruleSet,
      boolean ignoreModernDialects,
      String canonicalString
  ) {
    this.jsonNode = jsonNode;
    this.skemaObj = skemaObj;
    this.schemaObj = schemaObj;
    this.version = version;
    this.references = references;
    this.resolvedReferences = resolvedReferences;
    this.metadata = metadata;
    this.ruleSet = ruleSet;
    this.ignoreModernDialects = ignoreModernDialects;
    this.canonicalString = canonicalString;
  }

  @Override
  public JsonSchema copy() {
    return new JsonSchema(
        this.jsonNode,
        this.skemaObj,
        this.schemaObj,
        this.version,
        this.references,
        this.resolvedReferences,
        this.metadata,
        this.ruleSet,
        this.ignoreModernDialects,
        this.canonicalString
    );
  }

  @Override
  public JsonSchema copy(Integer version) {
    return new JsonSchema(
        this.jsonNode,
        this.skemaObj,
        this.schemaObj,
        version,
        this.references,
        this.resolvedReferences,
        this.metadata,
        this.ruleSet,
        this.ignoreModernDialects,
        this.canonicalString
    );
  }

  @Override
  public JsonSchema copy(Metadata metadata, RuleSet ruleSet) {
    return new JsonSchema(
        this.jsonNode,
        this.skemaObj,
        this.schemaObj,
        this.version,
        this.references,
        this.resolvedReferences,
        metadata,
        ruleSet,
        this.ignoreModernDialects,
        this.canonicalString
    );
  }

  @Override
  public ParsedSchema copy(Map<SchemaEntity, Set<String>> tagsToAdd,
                           Map<SchemaEntity, Set<String>> tagsToRemove) {
    JsonSchema schemaCopy = this.copy();
    JsonNode original = schemaCopy.toJsonNode().deepCopy();
    modifySchemaTags(original, tagsToAdd, tagsToRemove);
    return new JsonSchema(original.toString(),
      schemaCopy.references(),
      schemaCopy.resolvedReferences(),
      schemaCopy.metadata(),
      schemaCopy.ruleSet(),
      schemaCopy.version());
  }

  public JsonSchema copyIgnoringModernDialects() {
    return new JsonSchema(
        this.jsonNode,
        null,
        null,
        this.version,
        this.references,
        this.resolvedReferences,
        this.metadata,
        this.ruleSet,
        true,
        this.canonicalString
    );
  }

  public JsonNode toJsonNode() {
    return jsonNode;
  }

  @Override
  public Schema rawSchema() {
    if (jsonNode == null) {
      return null;
    }
    if (schemaObj == null) {
      try {
        if (jsonNode.isBoolean()) {
          schemaObj = jsonNode.booleanValue()
              ? TrueSchema.builder().build()
              : FalseSchema.builder().build();
        } else {
          // Extract the $schema to use for determining the id keyword
          SpecificationVersion spec = SpecificationVersion.DRAFT_7;
          if (jsonNode.has(SCHEMA_KEYWORD)) {
            String schema = jsonNode.get(SCHEMA_KEYWORD).asText();
            SpecificationVersion s = SpecificationVersion.getFromUrl(schema);
            if (s != null) {
              spec = s;
            }
          }
          switch (spec) {
            case DRAFT_2020_12:
            case DRAFT_2019_09:
              if (ignoreModernDialects) {
                loadPreviousDraft(spec);
              } else {
                loadLatestDraft();
              }
              break;
            default:
              loadPreviousDraft(spec);
              break;
          }
        }
      } catch (Throwable e) {
        throw new IllegalArgumentException("Invalid JSON Schema", e);
      }
    }
    return schemaObj;
  }

  @VisibleForTesting
  protected Map<URI, String> getPrepopulatedMappings() {
    return prepopulatedMetaSchemas;
  }

  private void loadLatestDraft() throws URISyntaxException {
    Map<URI, String> mappings = new HashMap<>(getPrepopulatedMappings());
    for (Map.Entry<String, String> dep : resolvedReferences.entrySet()) {
      URI uri = new URI(dep.getKey());
      mappings.put(uri, dep.getValue());
      if (!uri.isAbsolute() && !dep.getKey().startsWith(".")) {
        // For backward compatibility
        mappings.put(new URI("./" + dep.getKey()), dep.getValue());
      }
    }
    SchemaLoaderConfig config = SchemaLoaderConfig.createDefaultConfig(mappings);
    JsonValue schemaJson = objectMapper.convertValue(jsonNode, JsonObject.class);
    skemaObj = new com.github.erosb.jsonsKema.SchemaLoader(schemaJson, config).load();
    SchemaTranslator.SchemaContext ctx = skemaObj.accept(new SchemaTranslator());
    assert ctx != null;
    ctx.close();
    schemaObj = ctx.schema();
  }

  private void loadPreviousDraft(SpecificationVersion spec)
      throws JsonProcessingException {
    org.everit.json.schema.loader.SpecificationVersion loaderSpec =
        org.everit.json.schema.loader.SpecificationVersion.DRAFT_7;
    switch (spec) {
      case DRAFT_7:
        loaderSpec = org.everit.json.schema.loader.SpecificationVersion.DRAFT_7;
        break;
      case DRAFT_6:
        loaderSpec = org.everit.json.schema.loader.SpecificationVersion.DRAFT_6;
        break;
      case DRAFT_4:
        loaderSpec = org.everit.json.schema.loader.SpecificationVersion.DRAFT_4;
        break;
      default:
        break;
    }

    // Extract the $id to use for resolving relative $ref URIs
    URI idUri = null;
    if (jsonNode.has(loaderSpec.idKeyword())) {
      String id = jsonNode.get(loaderSpec.idKeyword()).asText();
      if (id != null) {
        idUri = ReferenceResolver.resolve((URI) null, id);
      }
    }
    SchemaLoader.SchemaLoaderBuilder builder = SchemaLoader.builder()
        .useDefaults(true).draftV7Support();
    for (Map.Entry<String, String> dep : resolvedReferences.entrySet()) {
      URI child = ReferenceResolver.resolve(idUri, dep.getKey());
      builder.registerSchemaByURI(child, new JSONObject(dep.getValue()));
    }
    JSONObject jsonObject = objectMapper.treeToValue(jsonNode, JSONObject.class);
    builder.schemaJson(jsonObject);
    SchemaLoader loader = builder.build();
    schemaObj = loader.load().build();
  }

  @Override
  public boolean hasTopLevelField(String field) {
    if (jsonNode != null) {
      JsonNode properties = jsonNode.get(PROPERTIES_KEYWORD);
      return properties instanceof ObjectNode && properties.has(field);
    }
    return false;
  }

  @Override
  public String schemaType() {
    return TYPE;
  }

  @Override
  public String name() {
    return getString("title");
  }

  public boolean has(String key) {
    return jsonNode.has(key);
  }

  public String getString(String key) {
    return jsonNode.has(key) ? jsonNode.get(key).asText() : null;
  }

  @Override
  public String canonicalString() {
    if (jsonNode == null) {
      return null;
    }
    if (canonicalString == null) {
      try {
        canonicalString = objectMapper.writeValueAsString(jsonNode);
      } catch (IOException e) {
        throw new IllegalArgumentException("Invalid JSON", e);
      }
    }
    return canonicalString;
  }

  @Override
  public Integer version() {
    return version;
  }

  @Override
  public List<SchemaReference> references() {
    return references;
  }

  public Map<String, String> resolvedReferences() {
    return resolvedReferences;
  }

  @Override
  public Metadata metadata() {
    return metadata;
  }

  @Override
  public RuleSet ruleSet() {
    return ruleSet;
  }

  public boolean isIgnoreModernDialects() {
    return ignoreModernDialects;
  }

  @Override
  public JsonSchema normalize() {
    String canonical = canonicalString();
    if (canonical == null) {
      return this;
    }
    try {
      JsonNode jsonNode = objectMapperWithOrderedProps.readTree(canonical);
      return new JsonSchema(
          jsonNode,
          this.references.stream().sorted().distinct().collect(Collectors.toList()),
          this.resolvedReferences,
          this.version
      );
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid JSON", e);
    }
  }

  @Override
  public void validate(boolean strict) {
    // Access the raw schema since it is computed lazily
    Schema rawSchema = rawSchema();
    if (strict) {
      if (rawSchema instanceof ObjectSchema) {
        ObjectSchema schema = (ObjectSchema) rawSchema;
        Optional<String> restrictedField = schema.getPropertySchemas()
                .keySet()
                .stream()
                .filter(field -> field.startsWith("$$"))
                .findAny();
        if (restrictedField.isPresent()) {
          throw new ValidationException(schema,
                  "Field names cannot start with $$ prefix",
                  "properties",
                  String.format("#/properties/%s", restrictedField.get()));
        }
      }
    }
  }

  public JsonNode validate(JsonNode value) throws JsonProcessingException, ValidationException {
    // Obtain the raw schema to ensure skemaObj is populated
    Schema rawSchema = rawSchema();
    if (skemaObj != null) {
      return validate(skemaObj, value);
    } else {
      return validate(rawSchema, value);
    }
  }

  public static JsonNode validate(com.github.erosb.jsonsKema.Schema schema, JsonNode value)
      throws JsonProcessingException, ValidationException {
    Validator validator = Validator.forSchema(schema);
    JsonValue primitiveValue = null;
    if (value instanceof BinaryNode) {
      primitiveValue = new JsonString(value.asText(), UnknownSource.INSTANCE);
    } else if (value instanceof BooleanNode) {
      primitiveValue = new JsonBoolean(value.asBoolean(), UnknownSource.INSTANCE);
    } else if (value instanceof NullNode) {
      primitiveValue = new JsonNull(UnknownSource.INSTANCE);
    } else if (value instanceof NumericNode) {
      primitiveValue = new JsonNumber(value.numberValue(), UnknownSource.INSTANCE);
    } else if (value instanceof TextNode) {
      primitiveValue = new JsonString(value.asText(), UnknownSource.INSTANCE);
    }
    ValidationFailure failure;
    if (primitiveValue != null) {
      failure = validator.validate(primitiveValue);
    } else {
      JsonValue jsonObject;
      if (value instanceof ArrayNode) {
        jsonObject = objectMapper.convertValue(value, JsonArray.class);
      } else {
        jsonObject = objectMapper.convertValue(value, JsonObject.class);
      }
      failure = validator.validate(jsonObject);
    }
    if (failure != null) {
      throw new ValidationException(failure.toString());
    }
    return value;
  }

  public static JsonNode validate(Schema schema, Object value)
      throws JsonProcessingException, ValidationException {
    Object primitiveValue = NONE_MARKER;
    if (isPrimitive(value)) {
      primitiveValue = value;
    } else if (value instanceof BinaryNode) {
      primitiveValue = ((BinaryNode) value).asText();
    } else if (value instanceof BooleanNode) {
      primitiveValue = ((BooleanNode) value).asBoolean();
    } else if (value instanceof NullNode) {
      primitiveValue = null;
    } else if (value instanceof NumericNode) {
      primitiveValue = ((NumericNode) value).numberValue();
    } else if (value instanceof TextNode) {
      primitiveValue = ((TextNode) value).asText();
    }
    if (primitiveValue != NONE_MARKER) {
      schema.validate(primitiveValue);
      return value instanceof JsonNode
          ? (JsonNode) value
          : objectMapper.convertValue(primitiveValue, JsonNode.class);
    } else {
      Object jsonObject;
      if (value instanceof ArrayNode) {
        jsonObject = objectMapper.treeToValue(((ArrayNode) value), JSONArray.class);
      } else if (value instanceof JsonNode) {
        jsonObject = objectMapper.treeToValue(((JsonNode) value), JSONObject.class);
      } else if (value.getClass().isArray()) {
        jsonObject = objectMapper.convertValue(value, JSONArray.class);
      } else {
        jsonObject = objectMapper.convertValue(value, JSONObject.class);
      }
      schema.validate(jsonObject);
      return objectMapper.convertValue(jsonObject, JsonNode.class);
    }
  }

  private static boolean isPrimitive(Object value) {
    return value == null
        || value instanceof Boolean
        || value instanceof Number
        || value instanceof String;
  }

  @Override
  public List<String> isBackwardCompatible(ParsedSchema previousSchema) {
    if (!schemaType().equals(previousSchema.schemaType())) {
      return Lists.newArrayList("Incompatible because of different schema type");
    }
    final List<Difference> differences =
            SchemaDiff.compare(((JsonSchema) previousSchema).rawSchema(), rawSchema());
    final List<Difference> incompatibleDiffs = differences.stream()
        .filter(diff -> !SchemaDiff.COMPATIBLE_CHANGES.contains(diff.getType()))
        .collect(Collectors.toList());
    boolean isCompatible = incompatibleDiffs.isEmpty();
    if (!isCompatible) {
      List<String> errorMessages = new ArrayList<>();
      for (Difference incompatibleDiff : incompatibleDiffs) {
        errorMessages.add(incompatibleDiff.toString());
      }
      return errorMessages;
    } else {
      return new ArrayList<>();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JsonSchema that = (JsonSchema) o;
    return Objects.equals(version, that.version)
        && Objects.equals(references, that.references)
        && Objects.equals(canonicalString(), that.canonicalString())
        && Objects.equals(metadata, that.metadata)
        && Objects.equals(ruleSet, that.ruleSet)
        && ignoreModernDialects == that.ignoreModernDialects;
  }

  @Override
  public int hashCode() {
    if (hashCode == NO_HASHCODE) {
      hashCode = Objects.hash(
          jsonNode, references, version, metadata, ruleSet, ignoreModernDialects);
    }
    return hashCode;
  }

  @Override
  public String toString() {
    return canonicalString();
  }

  @Override
  public Object fromJson(JsonNode json) {
    return json;
  }

  @Override
  public JsonNode toJson(Object message) throws IOException {
    if (message instanceof JsonNode) {
      return (JsonNode) message;
    }
    return objectMapper.readTree(JsonSchemaUtils.toJson(message));
  }

  @Override
  public Object copyMessage(Object message) throws IOException {
    if (message instanceof JsonNode) {
      return ((JsonNode) message).deepCopy();
    }
    return toJson(message);
  }

  @Override
  public Object transformMessage(RuleContext ctx, FieldTransform transform, Object message)
      throws RuleException {
    try {
      return toTransformedMessage(ctx, rawSchema(), "$", message, transform);
    } catch (RuntimeException e) {
      if (e.getCause() instanceof RuleException) {
        throw (RuleException) e.getCause();
      } else {
        throw e;
      }
    }
  }

  private Object toTransformedMessage(
      RuleContext ctx, Schema schema, String path, Object message, FieldTransform transform) {
    FieldContext fieldCtx = ctx.currentField();
    if (schema == null) {
      return message;
    }
    message = getValue(message);
    if (fieldCtx != null) {
      fieldCtx.setType(getType(schema));
    }
    if (schema instanceof CombinedSchema) {
      JsonNode jsonNode = objectMapper.convertValue(message, JsonNode.class);
      for (Schema subschema : ((CombinedSchema) schema).getSubschemas()) {
        boolean valid = false;
        try {
          validate(subschema, jsonNode);
          valid = true;
        } catch (Exception e) {
          // noop
        }
        if (valid) {
          return toTransformedMessage(ctx, subschema, path, message, transform);
        }
      }
      return message;
    } else if (schema instanceof ArraySchema) {
      if (!(message instanceof Iterable)) {
        log.warn("Object does not match an array schema");
        return message;
      }
      Schema subschema = ((ArraySchema)schema).getAllItemSchema();
      List<Object> result = new ArrayList<>();
      int i = 0;
      for (Object o : (Iterable<?>) message) {
        result.add(toTransformedMessage(ctx, subschema, path + "[" + i + "]", o, transform));
        i++;
      }
      return result;
    } else if (schema instanceof ObjectSchema) {
      if (message == null) {
        return null;
      }
      Map<String, Schema> propertySchemas = ((ObjectSchema) schema).getPropertySchemas();
      for (Map.Entry<String, Schema> entry : propertySchemas.entrySet()) {
        String propertyName = entry.getKey();
        Schema propertySchema = entry.getValue();
        String fullName = path + "." + propertyName;
        try (FieldContext fc = ctx.enterField(message, fullName, propertyName,
            getType(propertySchema), getInlineTags(propertySchema))) {
          PropertyAccessor propertyAccessor =
              getPropertyAccessor(ctx, message, propertyName);
          if (propertyAccessor == null) {
            continue;
          }
          Object value = propertyAccessor.getPropertyValue();
          Object newValue = toTransformedMessage(ctx, propertySchema, fullName, value, transform);
          if (ctx.rule().getKind() == RuleKind.CONDITION) {
            if (Boolean.FALSE.equals(newValue)) {
              throw new RuntimeException(new RuleConditionException(ctx.rule()));
            }
          } else {
            propertyAccessor.setPropertyValue(newValue);
          }
        }
      }
      return message;
    } else if (schema instanceof ReferenceSchema) {
      if (message == null) {
        return null;
      }
      return toTransformedMessage(ctx, ((ReferenceSchema)schema).getReferredSchema(),
          path, message, transform);
    } else if (schema instanceof ConditionalSchema
        || schema instanceof EmptySchema
        || schema instanceof FalseSchema
        || schema instanceof NotSchema) {
      return message;
    } else {
      if (fieldCtx != null) {
        try {
          Set<String> ruleTags = ctx.rule().getTags();
          if (ruleTags.isEmpty()) {
            return transform.transform(ctx, fieldCtx, message);
          } else {
            if (!RuleContext.disjoint(fieldCtx.getTags(), ruleTags)) {
              return transform.transform(ctx, fieldCtx, message);
            }
          }
        } catch (RuleException e) {
          throw new RuntimeException(e);
        }
      }
      return message;
    }
  }

  private Object getValue(Object message) {
    if (message instanceof TextNode) {
      return ((TextNode) message).asText();
    } else if (message instanceof NumericNode) {
      return ((NumericNode)message).numberValue();
    } else if (message instanceof BooleanNode) {
      return ((BooleanNode) message).asBoolean();
    } else if (message instanceof NullNode) {
      return null;
    } else {
      return message;
    }
  }

  private RuleContext.Type getType(Schema schema) {
    if (schema instanceof ObjectSchema) {
      return isMap((ObjectSchema) schema) ? Type.MAP : Type.RECORD;
    } else if (schema instanceof ConstSchema || schema instanceof EnumSchema) {
      return Type.ENUM;
    } else if (schema instanceof ArraySchema) {
      return Type.ARRAY;
    } else if (schema instanceof CombinedSchema) {
      return Type.COMBINED;
    } else if (schema instanceof StringSchema) {
      return Type.STRING;
    } else if (schema instanceof NumberSchema) {
      NumberSchema numberSchema = (NumberSchema) schema;
      return numberSchema.requiresInteger() ? Type.INT : Type.DOUBLE;
    } else if (schema instanceof BooleanSchema) {
      return Type.BOOLEAN;
    } else {
      return Type.NULL;
    }
  }

  private static boolean isMap(final ObjectSchema objectSchema) {
    return objectSchema.getPropertySchemas() == null
        || objectSchema.getPropertySchemas().isEmpty();
  }

  @Override
  public Set<String> inlineTags() {
    Set<String> tags = new LinkedHashSet<>();
    if (jsonNode == null) {
      return tags;
    }
    getInlineTagsRecursively(tags, jsonNode);
    return tags;
  }

  private void getInlineTagsRecursively(Set<String> tags, JsonNode node) {
    tags.addAll(getInlineTags(node));
    node.forEach(n -> getInlineTagsRecursively(tags, n));
  }

  @Override
  public Map<SchemaEntity, Set<String>> inlineTaggedEntities() {
    Map<SchemaEntity, Set<String>> tags = new LinkedHashMap<>();
    Schema schema = rawSchema();
    if (schema == null) {
      return tags;
    }
    getInlineTaggedEntitiesRecursively(tags, schema, "", false, new HashSet<>());
    return tags;
  }

  private void getInlineTaggedEntitiesRecursively(Map<SchemaEntity, Set<String>> tags,
      Schema schema, String scope, boolean inField, Set<String> visited) {
    if (schema instanceof CombinedSchema) {
      CombinedSchema combinedSchema = (CombinedSchema) schema;
      String scopedName = scope + JsonSchemaComparator.getCriterion(combinedSchema);
      List<Schema> subschemas = new ArrayList<>(combinedSchema.getSubschemas());
      subschemas.sort(new JsonSchemaComparator());
      for (int i = 0; i < subschemas.size(); i++) {
        Schema subschema = subschemas.get(i);
        getInlineTaggedEntitiesRecursively(
            tags, subschema, scopedName + "." + i + ".", false, visited);
      }
    } else if (schema instanceof ArraySchema) {
      Schema subschema = ((ArraySchema) schema).getAllItemSchema();
      getInlineTaggedEntitiesRecursively(tags, subschema, scope + "array.", false, visited);
    } else if (schema instanceof ObjectSchema) {
      ObjectSchema objectSchema = (ObjectSchema) schema;
      String scopedName = scope + "object";
      if (visited.contains(scopedName)) {
        return;
      } else {
        visited.add(scopedName);
      }
      if (!inField) {
        Set<String> recordTags = getInlineTags(schema);
        if (!recordTags.isEmpty()) {
          tags.put(new SchemaEntity(scopedName, SR_RECORD), recordTags);
        }
      }
      for (Map.Entry<String, Schema> entry : objectSchema.getPropertySchemas().entrySet()) {
        String propertyName = entry.getKey();
        Schema propertySchema = entry.getValue();
        String scopedPropertyName = scopedName + "." + propertyName;
        Set<String> fieldTags = getInlineTags(propertySchema);
        if (!fieldTags.isEmpty()) {
          tags.put(new SchemaEntity(scopedPropertyName, SR_FIELD), fieldTags);
        }
        getInlineTaggedEntitiesRecursively(
            tags, propertySchema, scopedPropertyName + ".", true, visited);
      }
      getInlineTaggedEntitiesRecursively(
          tags, schema.getUnprocessedProperties(), scopedName + ".", visited);
    } else if (schema instanceof ConditionalSchema) {
      ConditionalSchema condSchema = (ConditionalSchema) schema;
      String scopedName = scope + "conditional";
      condSchema.getIfSchema().ifPresent(value -> getInlineTaggedEntitiesRecursively(
          tags, value, scopedName + ".if.", false, visited));
      condSchema.getThenSchema().ifPresent(value -> getInlineTaggedEntitiesRecursively(
          tags, value, scopedName + ".then.", false, visited));
      condSchema.getElseSchema().ifPresent(value -> getInlineTaggedEntitiesRecursively(
          tags, value, scopedName + ".else.", false, visited));
    } else if (schema instanceof NotSchema) {
      Schema subschema = ((NotSchema) schema).getMustNotMatch();
      getInlineTaggedEntitiesRecursively(tags, subschema, scope + "not.", false, visited);
    }
  }

  private void getInlineTaggedEntitiesRecursively(Map<SchemaEntity, Set<String>> tags,
      Map<String, Object> unprocessedProperties, String scope, Set<String> visited) {
    Map<String, Object> defns = (Map<String, Object>) unprocessedProperties.get("definitions");
    if (defns != null) {
      for (Map.Entry<String, Object> entry : defns.entrySet()) {
        Object rawSchema = null;
        if (entry.getValue() instanceof Map) {
          rawSchema = replaceRefs((Map<String, Object>) entry.getValue());
        } else if (entry.getValue() instanceof List) {
          rawSchema = replaceRefs((List<Object>) entry.getValue());
        }
        if (rawSchema != null) {
          JsonNode jsonNode = objectMapper.valueToTree(rawSchema);
          JsonSchema jsonSchema = new JsonSchema(jsonNode);
          getInlineTaggedEntitiesRecursively(tags, jsonSchema.rawSchema(),
              scope + "definitions." + entry.getKey() + ".", false, visited);
        }
      }
    }
    Map<String, Object> defs = (Map<String, Object>) unprocessedProperties.get("$defs");
    if (defs != null) {
      for (Map.Entry<String, Object> entry : defs.entrySet()) {
        if (entry.getValue() instanceof Schema) {
          getInlineTaggedEntitiesRecursively(
              tags, (Schema) entry.getValue(),
              scope + "$defs." + entry.getKey() + ".", false, visited);
        }
      }
    }
  }

  private Map<String, Object> replaceRefs(Map<String, Object> defs) {
    Map<String, Object> result = new HashMap<>(defs);
    if (result.containsKey("$ref")) {
      // Use bare fragment as we don't care about the ref value during indexing
      result.put("$ref", "#");
    }
    for (Map.Entry<String, Object> entry : result.entrySet()) {
      if (entry.getValue() instanceof Map) {
        entry.setValue(replaceRefs((Map<String, Object>) entry.getValue()));
      } else if (entry.getValue() instanceof List) {
        entry.setValue(replaceRefs((List<Object>) entry.getValue()));
      } else if (entry.getValue() == JSONObject.NULL) {
        entry.setValue(null);
      }
    }
    return result;
  }

  private List<Object> replaceRefs(List<Object> items) {
    List<Object> result = new ArrayList<>();
    for (Object item : items) {
      if (item instanceof Map) {
        result.add(replaceRefs((Map<String, Object>) item));
      } else if (item instanceof List) {
        result.add(replaceRefs((List<Object>) item));
      } else if (item == JSONObject.NULL) {
        result.add(null);
      } else {
        result.add(item);
      }
    }
    return result;
  }

  private Set<String> getInlineTags(Schema schema) {
    Object prop = schema.getUnprocessedProperties().get(TAGS);
    if (prop instanceof List) {
      List<?> tags = (List<?>) prop;
      Set<String> result = new LinkedHashSet<>(tags.size());
      for (Object tag : tags) {
        result.add(tag.toString());
      }
      return result;
    }
    return Collections.emptySet();
  }

  private Set<String> getInlineTags(JsonNode tagNode) {
    Set<String> tags = new LinkedHashSet<>();
    if (tagNode.has(TAGS)) {
      ArrayNode tagArray = (ArrayNode) tagNode.get(TAGS);
      tagArray.elements().forEachRemaining(tag -> tags.add(tag.asText()));
    }
    return tags;
  }

  interface PropertyAccessor {
    Object getPropertyValue();

    void setPropertyValue(Object value);
  }

  private static PropertyAccessor getPropertyAccessor(
      RuleContext ctx, Object message, String propertyName) {
    if (message instanceof ObjectNode) {
      return new PropertyAccessor() {
        @Override
        public Object getPropertyValue() {
          return ((ObjectNode) message).get(propertyName);
        }

        @Override
        public void setPropertyValue(Object value) {
          ObjectNode objectNode = (ObjectNode) message;
          if (value instanceof List) {
            ArrayNode arrayNode = objectNode.putArray(propertyName);
            ((List<?>) value).forEach(v -> addArrayValue(arrayNode, v));
          } else if (value instanceof JsonNode) {
            objectNode.set(propertyName, (JsonNode) value);
          } else if (value instanceof Boolean) {
            objectNode.put(propertyName, (Boolean) value);
          } else if (value instanceof BigDecimal) {
            objectNode.put(propertyName, (BigDecimal) value);
          } else if (value instanceof BigInteger) {
            objectNode.put(propertyName, (BigInteger) value);
          } else if (value instanceof Long) {
            objectNode.put(propertyName, (Long) value);
          } else if (value instanceof Double) {
            objectNode.put(propertyName, (Double) value);
          } else if (value instanceof Float) {
            objectNode.put(propertyName, (Float) value);
          } else if (value instanceof Integer) {
            objectNode.put(propertyName, (Integer) value);
          } else if (value instanceof Short) {
            objectNode.put(propertyName, (Short) value);
          } else if (value instanceof Byte) {
            objectNode.put(propertyName, (Byte) value);
          } else if (value instanceof byte[]) {
            objectNode.put(propertyName, (byte[]) value);
          } else if (value == null) {
            objectNode.putNull(propertyName);
          } else {
            objectNode.put(propertyName, value.toString());
          }
        }

        private void addArrayValue(ArrayNode arrayNode, Object value) {
          if (value instanceof JsonNode) {
            arrayNode.add((JsonNode) value);
          } else if (value instanceof Boolean) {
            arrayNode.add((Boolean) value);
          } else if (value instanceof BigDecimal) {
            arrayNode.add((BigDecimal) value);
          } else if (value instanceof BigInteger) {
            arrayNode.add((BigInteger) value);
          } else if (value instanceof Long) {
            arrayNode.add((Long) value);
          } else if (value instanceof Double) {
            arrayNode.add((Double) value);
          } else if (value instanceof Float) {
            arrayNode.add((Float) value);
          } else if (value instanceof Integer) {
            arrayNode.add((Integer) value);
          } else if (value instanceof Short) {
            arrayNode.add((Short) value);
          } else if (value instanceof Byte) {
            arrayNode.add((Byte) value);
          } else if (value instanceof byte[]) {
            arrayNode.add((byte[]) value);
          } else {
            arrayNode.add(value.toString());
          }
        }
      };
    } else {
      BeanPropertyWriter getter = getBeanGetter(ctx, message, propertyName);
      SettableBeanProperty setter = getBeanSetter(ctx, message, propertyName);
      if (getter == null || setter == null) {
        return null;
      }
      return new PropertyAccessor() {
        @Override
        public Object getPropertyValue() {
          try {
            return getter.get(message);
          } catch (Exception e) {
            throw new IllegalStateException("Could not get property " + propertyName, e);
          }
        }

        @Override
        public void setPropertyValue(Object value) {
          try {
            if (value instanceof Number) {
              // Ideally this conversion would happen after calling the field transformer,
              // so that conversions would happen for these primitives when in arrays,
              // but JSON Schema can't represent all the Number subclasses.
              Number num = (Number) value;
              Class<?> cls = setter.getType().getRawClass();
              if (cls == byte.class || cls == Byte.class) {
                value = num.byteValue();
              } else if (cls == short.class || cls == Short.class) {
                value = num.shortValue();
              } else if (cls == int.class || cls == Integer.class) {
                value = num.intValue();
              } else if (cls == long.class || cls == Long.class) {
                value = num.longValue();
              } else if (cls == float.class || cls == Float.class) {
                value = num.floatValue();
              } else if (cls == double.class || cls == Double.class) {
                value = num.doubleValue();
              }
            }
            setter.set(message, value);
          } catch (IOException e) {
            throw new IllegalStateException("Could not set property " + propertyName, e);
          }
        }
      };
    }
  }

  private static BeanPropertyWriter getBeanGetter(
      RuleContext ctx, Object message, String propertyName) {
    Map<String, BeanPropertyWriter> props = beanGetters.computeIfAbsent(
        message.getClass().getName(),
        k -> {
          try {
            Map<String, BeanPropertyWriter> m = new HashMap<>();
            JsonSerializer<?> ser = objectMapper.getSerializerProviderInstance()
                .findValueSerializer(message.getClass());
            Iterator<PropertyWriter> propIter = ser.properties();
            while (propIter.hasNext()) {
              PropertyWriter p = propIter.next();
              if (p instanceof BeanPropertyWriter) {
                m.put(p.getName(), (BeanPropertyWriter) p);
              }
            }
            return m;
          } catch (Exception e) {
            throw new IllegalArgumentException(
                "Could not find JSON serializer for " + message.getClass(), e);
          }
        });
    return props.get(propertyName);
  }

  private static SettableBeanProperty getBeanSetter(
      RuleContext ctx, Object message, String propertyName) {
    Map<String, SettableBeanProperty> props = beanSetters.computeIfAbsent(
        message.getClass().getName(),
        k -> {
          try {
            Map<String, SettableBeanProperty> m = new HashMap<>();
            JavaType type = objectMapper.constructType(message.getClass());
            DeserializationContext ctxt = ((Impl) objectMapper.getDeserializationContext())
                .createDummyInstance(objectMapper.getDeserializationConfig());
            // Call findNonContextValueDeserializer instead of findRootValueDeserializer
            // so we don't get a wrapping TypeDeserializer
            JsonDeserializer<Object> deser = ctxt.findNonContextualValueDeserializer(type);
            if (deser instanceof BeanDeserializer) {
              Iterator<SettableBeanProperty> propIter = ((BeanDeserializer) deser).properties();
              while (propIter.hasNext()) {
                SettableBeanProperty p = propIter.next();
                m.put(p.getName(), p);
              }
            }
            return m;
          } catch (Exception e) {
            throw new IllegalArgumentException(
                "Could not find JSON deserializer for " + message.getClass(), e);
          }
        });
    return props.get(propertyName);
  }

  private void modifySchemaTags(JsonNode node,
                                Map<SchemaEntity, Set<String>> tagsToAddMap,
                                Map<SchemaEntity, Set<String>> tagsToRemoveMap) {
    Set<SchemaEntity> entityToModify = new LinkedHashSet<>(tagsToAddMap.keySet());
    entityToModify.addAll(tagsToRemoveMap.keySet());

    for (SchemaEntity entity : entityToModify) {
      JsonNode fieldNodePtr = findMatchingEntity(node, entity);
      Set<String> allTags = getInlineTags(fieldNodePtr);

      Set<String> tagsToAdd = tagsToAddMap.get(entity);
      if (tagsToAdd != null && !tagsToAdd.isEmpty()) {
        allTags.addAll(tagsToAdd);
      }

      Set<String> tagsToRemove = tagsToRemoveMap.get(entity);
      if (tagsToRemove != null && !tagsToRemove.isEmpty()) {
        allTags.removeAll(tagsToRemove);
      }

      if (allTags.isEmpty()) {
        ((ObjectNode) fieldNodePtr).remove(TAGS);
      } else {
        ((ObjectNode) fieldNodePtr).replace(TAGS, objectMapper.valueToTree(allTags));
      }
    }
  }

  private static String readFromClassPath(String absPath) {
    InputStream is = JsonSchema.class.getResourceAsStream(absPath);
    if (is != null) {
      BufferedReader reader = null;
      try {
        reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        return reader.lines().collect(Collectors.joining(System.lineSeparator()));
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException e) {
            // ignore
          }
        }
      }
    }
    throw new IllegalArgumentException("Could not load resource " + absPath);
  }
}
