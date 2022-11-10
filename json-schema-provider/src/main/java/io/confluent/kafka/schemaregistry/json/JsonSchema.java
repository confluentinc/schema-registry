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

import static io.confluent.kafka.schemaregistry.client.rest.entities.Metadata.EMPTY_METADATA;
import static io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet.EMPTY_RULESET;

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
import io.confluent.kafka.schemaregistry.rules.FieldTransform;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleContext.FieldContext;
import io.confluent.kafka.schemaregistry.rules.RuleContext.Type;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ConditionalSchema;
import org.everit.json.schema.EmptySchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.FalseSchema;
import org.everit.json.schema.NotSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.everit.json.schema.loader.SpecificationVersion;
import org.everit.json.schema.loader.internal.ReferenceResolver;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.ArrayList;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.diff.Difference;
import io.confluent.kafka.schemaregistry.json.diff.SchemaDiff;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;

public class JsonSchema implements ParsedSchema {

  private static final Logger log = LoggerFactory.getLogger(JsonSchema.class);

  public static final String TYPE = "JSON";

  public static final String ANNOTATIONS = "confluent.annotations";

  private static final String SCHEMA_KEYWORD = "$schema";

  private static final Object NONE_MARKER = new Object();

  private final JsonNode jsonNode;

  private transient Schema schemaObj;

  private final Integer version;

  private final List<SchemaReference> references;

  private final Map<String, String> resolvedReferences;

  private final Metadata metadata;

  private final RuleSet ruleSet;

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
    this.metadata = EMPTY_METADATA;
    this.ruleSet = EMPTY_RULESET;
  }

  public JsonSchema(
      String schemaString,
      List<SchemaReference> references,
      Map<String, String> resolvedReferences,
      Integer version
  ) {
    this(schemaString, references, resolvedReferences, EMPTY_METADATA, EMPTY_RULESET, version);
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
      this.jsonNode = objectMapper.readTree(schemaString);
      this.version = version;
      this.references = Collections.unmodifiableList(references);
      this.resolvedReferences = Collections.unmodifiableMap(resolvedReferences);
      this.metadata = metadata;
      this.ruleSet = ruleSet;
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
      this.metadata = EMPTY_METADATA;
      this.ruleSet = EMPTY_RULESET;
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid JSON " + schemaObj, e);
    }
  }

  private JsonSchema(
      JsonNode jsonNode,
      Schema schemaObj,
      Integer version,
      List<SchemaReference> references,
      Map<String, String> resolvedReferences,
      Metadata metadata,
      RuleSet ruleSet,
      String canonicalString
  ) {
    this.jsonNode = jsonNode;
    this.schemaObj = schemaObj;
    this.version = version;
    this.references = references;
    this.resolvedReferences = resolvedReferences;
    this.metadata = metadata;
    this.ruleSet = ruleSet;
    this.canonicalString = canonicalString;
  }

  @Override
  public JsonSchema copy() {
    return new JsonSchema(
        this.jsonNode,
        this.schemaObj,
        this.version,
        this.references,
        this.resolvedReferences,
        this.metadata,
        this.ruleSet,
        this.canonicalString
    );
  }

  @Override
  public JsonSchema copy(Integer version) {
    return new JsonSchema(
        this.jsonNode,
        this.schemaObj,
        version,
        this.references,
        this.resolvedReferences,
        this.metadata,
        this.ruleSet,
        this.canonicalString
    );
  }

  @Override
  public JsonSchema copy(Metadata metadata, RuleSet ruleSet) {
    return new JsonSchema(
        this.jsonNode,
        this.schemaObj,
        this.version,
        this.references,
        this.resolvedReferences,
        metadata,
        ruleSet,
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
        // Extract the $schema to use for determining the id keyword
        SpecificationVersion spec = SpecificationVersion.DRAFT_7;
        if (jsonNode.has(SCHEMA_KEYWORD)) {
          String schema = jsonNode.get(SCHEMA_KEYWORD).asText();
          if (schema != null) {
            spec = SpecificationVersion.lookupByMetaSchemaUrl(schema)
                    .orElse(SpecificationVersion.DRAFT_7);
          }
        }
        // Extract the $id to use for resolving relative $ref URIs
        URI idUri = null;
        if (jsonNode.has(spec.idKeyword())) {
          String id = jsonNode.get(spec.idKeyword()).asText();
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
      } catch (IOException e) {
        throw new IllegalArgumentException("Invalid JSON", e);
      }
    }
    return schemaObj;
  }

  @Override
  public String schemaType() {
    return TYPE;
  }

  @Override
  public String name() {
    return getString("title");
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
  public void validate() {
    // Access the raw schema since it is computed lazily
    rawSchema();
  }

  public void validate(Object value) throws JsonProcessingException, ValidationException {
    validate(rawSchema(), value);
  }

  private static void validate(Schema schema, Object value)
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
      return Collections.singletonList("Incompatible because of different schema type");
    }
    final List<Difference> differences = SchemaDiff.compare(
        ((JsonSchema) previousSchema).rawSchema(),
        rawSchema()
    );
    final List<Difference> incompatibleDiffs = differences.stream()
        .filter(diff -> !SchemaDiff.COMPATIBLE_CHANGES.contains(diff.getType()))
        .collect(Collectors.toList());
    boolean isCompatible = incompatibleDiffs.isEmpty();
    if (!isCompatible) {
      boolean first = true;
      List<String> errorMessages = new ArrayList<>();
      for (Difference incompatibleDiff : incompatibleDiffs) {
        if (first) {
          // Log first incompatible change as warning
          log.warn("Found incompatible change: {}", incompatibleDiff);
          errorMessages.add(String.format("Found incompatible change: %s", incompatibleDiff));
          first = false;
        } else {
          log.debug("Found incompatible change: {}", incompatibleDiff);
          errorMessages.add(String.format("Found incompatible change: %s", incompatibleDiff));
        }
      }
      return errorMessages;
    } else {
      return Collections.emptyList();
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
        && Objects.equals(ruleSet, that.ruleSet);
  }

  @Override
  public int hashCode() {
    if (hashCode == NO_HASHCODE) {
      hashCode = Objects.hash(jsonNode, references, version, metadata, ruleSet);
    }
    return hashCode;
  }

  @Override
  public String toString() {
    return canonicalString();
  }

  @Override
  public Object fromJson(JsonNode json) throws IOException {
    return json;
  }

  @Override
  public JsonNode toJson(Object message) throws IOException {
    if (message instanceof JsonNode) {
      return (JsonNode) message;
    }
    return JacksonMapper.INSTANCE.readTree(JsonSchemaUtils.toJson(message));
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
    if (schema == null || message == null) {
      return message;
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
      for (Iterator<? extends Object> it = ((Iterable<?>) message).iterator(); it.hasNext();) {
        result.add(toTransformedMessage(
            ctx, subschema, path + "[" + i + "]", it.next(), transform));
        i++;
      }
      return result;
    } else if (schema instanceof ObjectSchema) {
      Map<String, Schema> propertySchemas = ((ObjectSchema) schema).getPropertySchemas();
      for (Map.Entry<String, Schema> entry : propertySchemas.entrySet()) {
        String propertyName = entry.getKey();
        Schema propertySchema = entry.getValue();
        String fullName = path + "." + propertyName;
        try (FieldContext fc = ctx.enterField(ctx, message, fullName, propertyName,
            getType(propertySchema), getInlineAnnotations(propertySchema))) {
          PropertyAccessor propertyAccessor = getPropertyAccessor(ctx, message, propertyName);
          Object value = propertyAccessor.getPropertyValue();
          Object newValue = toTransformedMessage(ctx, propertySchema, fullName, value, transform);
          propertyAccessor.setPropertyValue(newValue);
        }
      }
      return message;
    } else if (schema instanceof ReferenceSchema) {
      return toTransformedMessage(ctx, ((ReferenceSchema)schema).getReferredSchema(),
          path, message, transform);
    } else if (schema instanceof ConditionalSchema
        || schema instanceof EmptySchema
        || schema instanceof FalseSchema
        || schema instanceof NotSchema
        || schema instanceof NullSchema) {
      return message;
    } else {
      FieldContext fc = ctx.currentField();
      if (fc != null) {
        try {
          Set<String> intersect = new HashSet<>(fc.getAnnotations());
          intersect.retainAll(ctx.rule().getAnnotations());
          if (!intersect.isEmpty()) {
            return transform.transform(ctx, fc, message);
          }
        } catch (RuleException e) {
          throw new RuntimeException(e);
        }
      }
      return message;
    }
  }

  private RuleContext.Type getType(Schema schema) {
    if (schema instanceof ObjectSchema) {
      return isMap((ObjectSchema) schema) ? Type.MAP : Type.RECORD;
    } else if (schema instanceof EnumSchema) {
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
        || objectSchema.getPropertySchemas().size() == 0;
  }

  private Set<String> getInlineAnnotations(Schema propertySchema) {
    Set<String> annotations = new HashSet<>();
    Object prop = propertySchema.getUnprocessedProperties().get(ANNOTATIONS);
    if (prop instanceof List) {
      ((List<?>)prop).forEach(p -> annotations.add(p.toString()));
    }
    return annotations;
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
          return (((ObjectNode) message).get(propertyName));
        }

        @Override
        public void setPropertyValue(Object value) {
          ObjectNode objectNode = (ObjectNode) message;
          if (value instanceof Boolean) {
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
          } else {
            objectNode.put(propertyName, value.toString());
          }
        }
      };
    } else {
      BeanPropertyWriter getter = getBeanGetter(ctx, message, propertyName);
      SettableBeanProperty setter = getBeanSetter(ctx, message, propertyName);
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
            setter.set(message, value);
          } catch (IOException e) {
            throw new IllegalStateException("Could not set property " + propertyName, e);
          }
        }
      };
    }
  }

  @SuppressWarnings("unchecked")
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

  @SuppressWarnings("unchecked")
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
            JsonDeserializer<Object> deser = ctxt.findRootValueDeserializer(type);
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
}
