/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.builtin;

import static io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity.EntityType.SR_FIELD;
import static io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity.EntityType.SR_RECORD;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.EnumHashBiMap;
import com.google.common.collect.Lists;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.builtin.converters.AvroConverter;
import io.confluent.kafka.schemaregistry.builtin.converters.FlinkConverter;
import io.confluent.kafka.schemaregistry.builtin.converters.ProtobufConverter;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rules.FieldTransform;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleContext.FieldContext;
import io.confluent.kafka.schemaregistry.rules.RuleContext.Type;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NativeSchema implements ParsedSchema {

  private static final Logger log = LoggerFactory.getLogger(NativeSchema.class);

  public static final String TYPE = "CFLT";
  public static final String TAGS = "tags";

  private final Schema schemaObj;
  private String canonicalString;
  private final Integer version;
  private final List<SchemaReference> references;
  private final Map<String, String> resolvedReferences;
  private final Metadata metadata;
  private final RuleSet ruleSet;

  private transient int hashCode = NO_HASHCODE;

  private static final int NO_HASHCODE = Integer.MIN_VALUE;

  private static final ObjectMapper jsonMapper = JacksonMapper.INSTANCE;

  public NativeSchema(String schemaString) {
    this(schemaString, Collections.emptyList(), Collections.emptyMap(), null);
  }

  public NativeSchema(String schemaString,
                      List<SchemaReference> references,
                      Map<String, String> resolvedReferences,
                      Integer version) {
    this(schemaString, references, resolvedReferences, null, null, version);
  }

  public NativeSchema(String schemaString,
                      List<SchemaReference> references,
                      Map<String, String> resolvedReferences,
                      Metadata metadata,
                      RuleSet ruleSet,
                      Integer version) {
    this.schemaObj = schemaString != null ? parseSchema(schemaString) : null;
    this.references = Collections.unmodifiableList(references);
    this.resolvedReferences = Collections.unmodifiableMap(resolvedReferences);
    this.metadata = metadata;
    this.ruleSet = ruleSet;
    this.version = version;
  }

  public NativeSchema(Schema schemaObj) {
    this(schemaObj, null);
  }

  public NativeSchema(Schema schemaObj, Integer version) {
    this.schemaObj = schemaObj;
    this.references = Collections.emptyList();
    this.resolvedReferences = Collections.emptyMap();
    this.metadata = null;
    this.ruleSet = null;
    this.version = version;
  }

  private NativeSchema(
      Schema schemaObj,
      String canonicalString,
      List<SchemaReference> references,
      Map<String, String> resolvedReferences,
      Metadata metadata,
      RuleSet ruleSet,
      Integer version
  ) {
    this.schemaObj = schemaObj;
    this.canonicalString = canonicalString;
    this.references = references;
    this.resolvedReferences = resolvedReferences;
    this.metadata = metadata;
    this.ruleSet = ruleSet;
    this.version = version;
  }

  @Override
  public NativeSchema copy() {
    return new NativeSchema(
        this.schemaObj,
        this.canonicalString,
        this.references,
        this.resolvedReferences,
        this.metadata,
        this.ruleSet,
        this.version
    );
  }

  @Override
  public NativeSchema copy(Integer version) {
    return new NativeSchema(
        this.schemaObj,
        this.canonicalString,
        this.references,
        this.resolvedReferences,
        this.metadata,
        this.ruleSet,
        version
    );
  }

  @Override
  public NativeSchema copy(Metadata metadata, RuleSet ruleSet) {
    return new NativeSchema(
        this.schemaObj,
        this.canonicalString,
        this.references,
        this.resolvedReferences,
        metadata,
        ruleSet,
        this.version
    );
  }

  @Override
  public ParsedSchema copy(Map<SchemaEntity, Set<String>> tagsToAdd,
                           Map<SchemaEntity, Set<String>> tagsToRemove) {
    NativeSchema schemaCopy = this.copy();
    JsonNode original;
    try {
      original = jsonMapper.readTree(schemaCopy.canonicalString());
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    // TODO RAY
    //modifySchemaTags(original, tagsToAdd, tagsToRemove);
    return new NativeSchema(original.toString(),
      schemaCopy.references(),
      schemaCopy.resolvedReferences(),
      schemaCopy.metadata(),
      schemaCopy.ruleSet(),
      schemaCopy.version());
  }

  @Override
  public Schema rawSchema() {
    return schemaObj;
  }

  @Override
  public boolean hasTopLevelField(String field) {
    return schemaObj != null && schemaObj.getType() == Schema.Type.STRUCT
            && schemaObj.getField(field) != null;
  }

  @Override
  public String schemaType() {
    return TYPE;
  }

  @Override
  public String name() {
    if (schemaObj != null && schemaObj.getType() == Schema.Type.STRUCT) {
      return schemaObj.getFullName();
    }
    return null;
  }

  @Override
  public String canonicalString() {
    if (schemaObj == null) {
      return null;
    }
    if (canonicalString == null) {
      List<Schema> schemaRefs = new ArrayList<>();
      for (String schema : resolvedReferences.values()) {
        Schema schemaRef = parseSchema(schema);
        schemaRefs.add(schemaRef);
      }
      canonicalString = schemaObj.toString();
    }
    return canonicalString;
  }

  private Schema parseSchema(String schemaString) {
    try {
      return jsonMapper.readValue(schemaString, Schema.class);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid schema string: " + schemaString, e);
    }
  }

  @Override
  public String formattedString(String format) {
    if (format == null || format.trim().isEmpty()) {
      return canonicalString();
    }
    Format formatEnum = Format.get(format.toLowerCase(Locale.ROOT));
    if (formatEnum == null) {
      // Don't throw an exception for forward compatibility of formats
      log.warn("Unsupported format {}", format);
      return canonicalString();
    }
    switch (formatEnum) {
      case DEFAULT:
        return canonicalString();
      case AVRO:
        AvroConverter avroConverter = new AvroConverter(100);
        AvroSchema avroSchema = avroConverter.fromNativeSchema(this);
        return avroSchema.canonicalString();
      case FLINK:
        FlinkConverter flinkConverter = new FlinkConverter(100);
        return flinkConverter.fromNativeSchema(this);
      case PROTOBUF:
        ProtobufConverter protobufConverter = new ProtobufConverter(100);
        ProtobufSchema protobufSchema = protobufConverter.fromNativeSchema(this);
        return protobufSchema.canonicalString();
      default:
        // Don't throw an exception for forward compatibility of formats
        log.warn("Unsupported format {}", format);
        return canonicalString();
    }
  }

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
  public NativeSchema normalize() {
    // TODO RAY
    /*
    String normalized = AvroSchemaUtils.toNormalizedString(this);
    return new NativeSchema(
        normalized,
        this.references.stream().sorted().distinct().collect(Collectors.toList()),
        this.resolvedReferences,
        this.version
    );
    */
    return this;
  }

  @Override
  public List<String> isBackwardCompatible(ParsedSchema previousSchema) {
    if (!schemaType().equals(previousSchema.schemaType())) {
      return Lists.newArrayList("Incompatible because of different schema type");
    }
    // TODO RAY
    /*
    try {
      SchemaCompatibility.SchemaPairCompatibility result =
          SchemaCompatibility.checkReaderWriterCompatibility(
              this.schemaObj,
              ((NativeSchema) previousSchema).schemaObj);
      return result.getResult().getIncompatibilities().stream()
              .map(Difference::new)
              .map(Difference::toString)
              .collect(Collectors.toCollection(ArrayList::new));
    } catch (Exception e) {
      log.error("Unexpected exception during compatibility check", e);
      return Lists.newArrayList(
              "Unexpected exception during compatibility check: " + e.getMessage());
    }
    */
    return Collections.emptyList();
  }

  /**
   * Returns whether the underlying raw representations are equivalent,
   * ignoring version and references.
   *
   * @return whether the underlying raw representations are equivalent
   */
  @Override
  public boolean equivalent(ParsedSchema schema) {
    if (this == schema) {
      return true;
    }
    if (schema == null || getClass() != schema.getClass()) {
      return false;
    }
    NativeSchema that = (NativeSchema) schema;
    return Objects.equals(schemaObj, that.schemaObj)
        && Objects.equals(metadata, that.metadata)
        && Objects.equals(ruleSet, that.ruleSet)
        && metaEqual(schemaObj, that.schemaObj, new HashMap<>());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NativeSchema that = (NativeSchema) o;
    return Objects.equals(version, that.version)
        && Objects.equals(references, that.references)
        && Objects.equals(schemaObj, that.schemaObj)
        && Objects.equals(metadata, that.metadata)
        && Objects.equals(ruleSet, that.ruleSet)
        && metaEqual(schemaObj, that.schemaObj, new HashMap<>());
  }

  private boolean metaEqual(
      Schema schema1, Schema schema2, Map<IdentityPair<Schema, Schema>, Boolean> cache) {
    if (schema1 == schema2) {
      return true;
    }

    if (schema1 == null || schema2 == null) {
      return false;
    }

    Schema.Type type1 = schema1.getType();
    Schema.Type type2 = schema2.getType();
    if (type1 != type2) {
      return false;
    }

    switch (type1) {
      case STRUCT:
        // Add a temporary value to the cache to avoid cycles.
        // As long as we recurse only at the end of the method, we can safely default to true here.
        // The cache is updated at the end of the method with the actual comparison result.
        IdentityPair<Schema, Schema> sp = new IdentityPair<>(schema1, schema2);
        Boolean cacheHit = cache.putIfAbsent(sp, true);
        if (cacheHit != null) {
          return cacheHit;
        }

        boolean equals = Objects.equals(schema1.getTags(), schema2.getTags())
            && Objects.equals(schema1.getDoc(), schema2.getDoc())
            && fieldMetaEqual(schema1.getFields(), schema2.getFields(), cache);

        cache.put(sp, equals);
        return equals;
      case ENUM:
        return Objects.equals(schema1.getTags(), schema2.getTags())
            && Objects.equals(schema1.getDoc(), schema2.getDoc())
            && Objects.equals(schema1.getEnumDefault(), schema2.getEnumDefault());
      case ARRAY:
      case MULTISET:
        return metaEqual(schema1.getElementType(), schema2.getElementType(), cache);
      case MAP:
        return metaEqual(schema1.getValueType(), schema2.getValueType(), cache);
      case BINARY:
        return Objects.equals(schema1.getTags(), schema2.getTags())
            && Objects.equals(schema1.getDoc(), schema2.getDoc());
      case UNION:
        List<Schema> types1 = schema1.getTypes();
        List<Schema> types2 = schema2.getTypes();
        if (types1.size() != types2.size()) {
          return false;
        }
        for (int i = 0; i < types1.size(); i++) {
          if (!metaEqual(types1.get(i), types2.get(i), cache)) {
            return false;
          }
        }
        return true;
      default:
        return true;
    }
  }

  private boolean fieldMetaEqual(
      List<Schema.Field> fields1,
      List<Schema.Field> fields2,
      Map<IdentityPair<Schema, Schema>, Boolean> cache) {
    if (fields1.size() != fields2.size()) {
      return false;
    }
    for (int i = 0; i < fields1.size(); i++) {
      Schema.Field field1 = fields1.get(i);
      Schema.Field field2 = fields2.get(i);
      if (field1 == field2) {
        continue;
      }
      if (!Objects.equals(field1.tags(), field2.tags())
          || !Objects.equals(field1.doc(), field2.doc())) {
        return false;
      }
      boolean fieldSchemaMetaEqual = metaEqual(field1.schema(), field2.schema(), cache);
      if (!fieldSchemaMetaEqual) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    if (hashCode == NO_HASHCODE) {
      hashCode = Objects.hash(schemaObj, references, version, metadata, ruleSet)
          + metaHash(schemaObj, new IdentityHashMap<>());
    }
    return hashCode;
  }

  private int metaHash(Schema schema, Map<Schema, Integer> cache) {
    if (schema == null) {
      return 0;
    }
    switch (schema.getType()) {
      case STRUCT:
        // Add a temporary value to the cache to avoid cycles.
        // As long as we recurse only at the end of the method, we can safely default to 0 here.
        // The cache is updated at the end of the method with the actual comparison result.
        Integer cacheHit = cache.putIfAbsent(schema, 0);
        if (cacheHit != null) {
          return cacheHit;
        }

        int result = Objects.hash(schema.getTags(), schema.getDoc())
            + fieldMetaHash(schema.getFields(), cache);

        cache.put(schema, result);
        return result;
      case ENUM:
        return Objects.hash(schema.getTags(), schema.getDoc(), schema.getEnumDefault());
      case ARRAY:
      case MULTISET:
        return metaHash(schema.getElementType(), cache);
      case MAP:
        return metaHash(schema.getValueType(), cache);
      case BINARY:
        return Objects.hash(schema.getTags(), schema.getDoc());
      case UNION:
        int hash = 0;
        List<Schema> types = schema.getTypes();
        for (Schema type : types) {
          hash += metaHash(type, cache);
        }
        return hash;
      default:
        return 0;
    }
  }

  private int fieldMetaHash(List<Schema.Field> fields, Map<Schema, Integer> cache) {
    int hash = 0;
    for (Schema.Field field : fields) {
      hash += Objects.hash(field.tags(), field.doc()) + metaHash(field.schema(), cache);
    }
    return hash;
  }

  @Override
  public String toString() {
    return canonicalString();
  }

  @Override
  public Object fromJson(JsonNode json) throws IOException {
    // TODO RAY
    /*
    return AvroSchemaUtils.toObject(json, this);
    */
    return null;
  }

  @Override
  public JsonNode toJson(Object message) throws IOException {
    if (message instanceof JsonNode) {
      return (JsonNode) message;
    }
    // TODO RAY
    /*
    return JacksonMapper.INSTANCE.readTree(AvroSchemaUtils.toJson(message));
    */
    return null;
  }

  @Override
  public Object copyMessage(Object message) {
    // TODO RAY
    /*
    GenericData data = AvroSchemaUtils.getData(rawSchema(), message, false, false);
    return data.deepCopy(rawSchema(), message);
    */
    return null;
  }

  @Override
  public Object transformMessage(RuleContext ctx, FieldTransform transform, Object message)
      throws RuleException {
    try {
      return toTransformedMessage(ctx, this.rawSchema(), message, transform);
    } catch (RuntimeException e) {
      if (e.getCause() instanceof RuleException) {
        throw (RuleException) e.getCause();
      } else {
        throw e;
      }
    }
  }

  private Object toTransformedMessage(
      RuleContext ctx, Schema schema, Object message, FieldTransform transform) {
    // TODO RAY
    //FieldContext fieldCtx = ctx.currentField();
    if (schema == null) {
      return message;
    }
    // TODO RAY
    /*
    if (fieldCtx != null) {
      fieldCtx.setType(getType(schema));
    }
    GenericData data;
    Schema.Type st = schema.getType();
    switch (st) {
      case UNION:
        data = AvroSchemaUtils.getData(schema, message, false, false);
        int unionIndex = data.resolveUnion(schema, message);
        return toTransformedMessage(ctx, schema.getTypes().get(unionIndex), message, transform);
      case ARRAY:
        if (!(message instanceof Iterable)) {
          log.warn("Object does not match an array schema");
          return message;
        }
        return StreamSupport.stream(((Iterable<?>) message).spliterator(), false)
            .map(it -> toTransformedMessage(ctx, schema.getElementType(), it, transform))
            .collect(Collectors.toList());
      case MAP:
      case MULTISET:
        if (!(message instanceof Map)) {
          log.warn("Object does not match a map schema");
          return message;
        }
        return ((Map<?, ?>) message).entrySet().stream()
            .collect(Collectors.toMap(
                Entry::getKey,
                e -> toTransformedMessage(ctx, schema.getValueType(), e.getValue(), transform),
                (e1, e2) -> e1));
      case STRUCT:
        if (message == null) {
          return null;
        }
        data = AvroSchemaUtils.getData(schema, message, false, false);
        for (Schema.Field f : schema.getFields()) {
          String fullName = schema.getFullName() + "." + f.name();
          try (FieldContext fc = ctx.enterField(
              message, fullName, f.name(), getType(f.schema()), getInlineTags(f))) {
            Object value = data.getField(message, f.name(), f.pos());
            if (value instanceof Utf8) {
              value = value.toString();
            }
            Object newValue = toTransformedMessage(ctx, f.schema(), value, transform);
            if (ctx.rule().getKind() == RuleKind.CONDITION) {
              if (Boolean.FALSE.equals(newValue)) {
                throw new RuntimeException(new RuleConditionException(ctx.rule()));
              }
            } else {
              data.setField(message, f.name(), f.pos(), newValue);
            }
          }
        }
        return message;
      default:
        if (fieldCtx != null) {
          try {
            Set<String> ruleTags = ctx.rule().getTags();
            if (ruleTags.isEmpty()) {
              return fieldTransform(ctx, message, transform, fieldCtx);
            } else {
              if (!RuleContext.disjoint(fieldCtx.getTags(), ruleTags)) {
                return fieldTransform(ctx, message, transform, fieldCtx);
              }
            }
          } catch (RuleException e) {
            throw new RuntimeException(e);
          }
        }
        return message;
    }
    */
    return null;
  }

  private static Object fieldTransform(RuleContext ctx, Object message, FieldTransform transform,
      FieldContext fieldCtx) throws RuleException {
    if (message instanceof ByteBuffer) {
      ByteBuffer buffer = (ByteBuffer) message;
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      message = bytes;
    }
    Object result = transform.transform(ctx, fieldCtx, message);
    if (result instanceof byte[]) {
      result = ByteBuffer.wrap((byte[]) result);
    }
    return result;
  }

  private Type getType(Schema schema) {
    switch (schema.getType()) {
      case STRUCT:
        return Type.RECORD;
      case ENUM:
        return Type.ENUM;
      case ARRAY:
        return Type.ARRAY;
      case MAP:
      case MULTISET:
        return Type.MAP;
      case UNION:
        return Type.COMBINED;
      case BINARY:
        return Type.FIXED;
      case CHAR:
        return Type.STRING;
      case STRING:
        return Type.STRING;
      case BYTES:
        return Type.BYTES;
      case INT8:
      case INT16:
      case INT32:
        return Type.INT;
      case INT64:
        return Type.LONG;
      case FLOAT32:
        return Type.FLOAT;
      case FLOAT64:
        return Type.DOUBLE;
      case BOOLEAN:
        return Type.BOOLEAN;
      case NULL:
      default:
        return Type.NULL;
    }
  }

  @Override
  public Set<String> inlineTags() {
    try {
      Set<String> tags = new LinkedHashSet<>();
      String canonicalString = canonicalString();
      if (canonicalString == null) {
        return tags;
      }
      JsonNode jsonNode = jsonMapper.readTree(canonicalString);
      getInlineTagsRecursively(tags, jsonNode);
      return tags;
    } catch (IOException e) {
      throw new IllegalStateException("Could not parse schema: " + canonicalString());
    }
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
    getInlineTaggedEntitiesRecursively(tags, schema, new HashSet<>());
    return tags;
  }

  private void getInlineTaggedEntitiesRecursively(
      Map<SchemaEntity, Set<String>> tags, Schema schema, Set<String> visited) {
    switch (schema.getType()) {
      case UNION:
        for (Schema subtype : schema.getTypes()) {
          getInlineTaggedEntitiesRecursively(tags, subtype, visited);
        }
        break;
      case ARRAY:
      case MULTISET:
        getInlineTaggedEntitiesRecursively(tags, schema.getElementType(), visited);
        break;
      case MAP:
        getInlineTaggedEntitiesRecursively(tags, schema.getValueType(), visited);
        break;
      case STRUCT:
        String fullName = schema.getFullName();
        if (visited.contains(fullName)) {
          return;
        } else {
          visited.add(fullName);
        }
        Set<String> recordTags = getInlineTags(schema);
        if (!recordTags.isEmpty()) {
          tags.put(new SchemaEntity(fullName, SR_RECORD), recordTags);
        }
        for (Schema.Field f : schema.getFields()) {
          Set<String> fieldTags = getInlineTags(f);
          if (!fieldTags.isEmpty()) {
            tags.put(new SchemaEntity(fullName + "." + f.name(), SR_FIELD), fieldTags);
          }
          getInlineTaggedEntitiesRecursively(tags, f.schema(), visited);
        }
        break;
      default:
        break;
    }
  }

  private Set<String> getInlineTags(Schema record) {
    Object prop = record.getTags();
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

  private Set<String> getInlineTags(Schema.Field field) {
    Object prop = field.tags();
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

  // TODO RAY
  /*
  private void modifySchemaTags(JsonNode node,
                                Map<SchemaEntity, Set<String>> tagsToAddMap,
                                Map<SchemaEntity, Set<String>> tagsToRemoveMap) {
    Set<SchemaEntity> entityToModify = new LinkedHashSet<>(tagsToAddMap.keySet());
    entityToModify.addAll(tagsToRemoveMap.keySet());

    for (SchemaEntity entity : entityToModify) {
      JsonNode nodePtr = AvroSchemaUtils.findMatchingEntity(node, entity);
      Set<String> allTags = getInlineTags(nodePtr);

      Set<String> tagsToAdd = tagsToAddMap.get(entity);
      if (tagsToAdd != null && !tagsToAdd.isEmpty()) {
        allTags.addAll(tagsToAdd);
      }

      Set<String> tagsToRemove = tagsToRemoveMap.get(entity);
      if (tagsToRemove != null && !tagsToRemove.isEmpty()) {
        allTags.removeAll(tagsToRemove);
      }

      if (allTags.isEmpty()) {
        ((ObjectNode) nodePtr).remove(TAGS);
      } else {
        ((ObjectNode) nodePtr).replace(TAGS, jsonMapper.valueToTree(allTags));
      }
    }
  }
  */

  static class IdentityPair<K, V> {
    private final K key;
    private final V value;

    public IdentityPair(K key, V value) {
      this.key = key;
      this.value = value;
    }

    public K getKey() {
      return key;
    }

    public V getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      IdentityPair<?, ?> pair = (IdentityPair<?, ?>) o;
      // Only perform identity check
      return key == pair.key && value == pair.value;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(key) + System.identityHashCode(value);
    }

    @Override
    public String toString() {
      return "IdentityPair{"
          + "key=" + key
          + ", value=" + value
          + '}';
    }
  }

  public enum Format {
    DEFAULT("default"),
    AVRO("avro"),
    PROTOBUF("protobuf"),
    JSON("json"),
    FLINK("flink"),
    ICEBERG("iceberg");

    private static final EnumHashBiMap<Format, String> lookup =
        EnumHashBiMap.create(Format.class);

    static {
      for (Format type : Format.values()) {
        lookup.put(type, type.symbol());
      }
    }

    private final String symbol;

    Format(String symbol) {
      this.symbol = symbol;
    }

    public String symbol() {
      return symbol;
    }

    public static Format get(String symbol) {
      return lookup.inverse().get(symbol);
    }

    public static Set<String> symbols() {
      return lookup.inverse().keySet();
    }

    @Override
    public String toString() {
      return symbol();
    }
  }
}
