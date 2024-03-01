/*
 * Copyright 2014 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.EnumHashBiMap;
import com.google.common.collect.Lists;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity;
import io.confluent.kafka.schemaregistry.rules.FieldTransform;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleContext.FieldContext;
import io.confluent.kafka.schemaregistry.rules.RuleContext.Type;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;

import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroSchema implements ParsedSchema {

  private static final Logger log = LoggerFactory.getLogger(AvroSchema.class);

  public static final String TYPE = "AVRO";
  public static final String TAGS = "confluent:tags";
  public static final String NAME_FIELD = "name";
  public static final String FIELDS_FIELD = "fields";

  private final Schema schemaObj;
  private String canonicalString;
  private final Integer version;
  private final List<SchemaReference> references;
  private final Map<String, String> resolvedReferences;
  private final Metadata metadata;
  private final RuleSet ruleSet;
  private final boolean isNew;

  private transient int hashCode = NO_HASHCODE;

  private static final int NO_HASHCODE = Integer.MIN_VALUE;

  private static final ObjectMapper jsonMapper = JacksonMapper.INSTANCE;

  public AvroSchema(String schemaString) {
    this(schemaString, Collections.emptyList(), Collections.emptyMap(), null);
  }

  public AvroSchema(String schemaString,
                    List<SchemaReference> references,
                    Map<String, String> resolvedReferences,
                    Integer version) {
    this(schemaString, references, resolvedReferences, version, false);
  }

  public AvroSchema(String schemaString,
                    List<SchemaReference> references,
                    Map<String, String> resolvedReferences,
                    Integer version,
                    boolean isNew) {
    this(schemaString, references, resolvedReferences, null, null, version, isNew);
  }

  public AvroSchema(String schemaString,
                    List<SchemaReference> references,
                    Map<String, String> resolvedReferences,
                    Metadata metadata,
                    RuleSet ruleSet,
                    Integer version,
                    boolean isNew) {
    this.isNew = isNew;
    Schema.Parser parser = getParser();
    for (String schema : resolvedReferences.values()) {
      parser.parse(schema);
    }
    this.schemaObj = schemaString != null ? parser.parse(schemaString) : null;
    this.references = Collections.unmodifiableList(references);
    this.resolvedReferences = Collections.unmodifiableMap(resolvedReferences);
    this.metadata = metadata;
    this.ruleSet = ruleSet;
    this.version = version;
  }

  public AvroSchema(Schema schemaObj) {
    this(schemaObj, null);
  }

  public AvroSchema(Schema schemaObj, Integer version) {
    this.isNew = false;
    this.schemaObj = schemaObj;
    this.references = Collections.emptyList();
    this.resolvedReferences = Collections.emptyMap();
    this.metadata = null;
    this.ruleSet = null;
    this.version = version;
  }

  private AvroSchema(
      Schema schemaObj,
      String canonicalString,
      List<SchemaReference> references,
      Map<String, String> resolvedReferences,
      Metadata metadata,
      RuleSet ruleSet,
      Integer version,
      boolean isNew
  ) {
    this.isNew = isNew;
    this.schemaObj = schemaObj;
    this.canonicalString = canonicalString;
    this.references = references;
    this.resolvedReferences = resolvedReferences;
    this.metadata = metadata;
    this.ruleSet = ruleSet;
    this.version = version;
  }

  @Override
  public AvroSchema copy() {
    return new AvroSchema(
        this.schemaObj,
        this.canonicalString,
        this.references,
        this.resolvedReferences,
        this.metadata,
        this.ruleSet,
        this.version,
        this.isNew
    );
  }

  @Override
  public AvroSchema copy(Integer version) {
    return new AvroSchema(
        this.schemaObj,
        this.canonicalString,
        this.references,
        this.resolvedReferences,
        this.metadata,
        this.ruleSet,
        version,
        this.isNew
    );
  }

  @Override
  public AvroSchema copy(Metadata metadata, RuleSet ruleSet) {
    return new AvroSchema(
        this.schemaObj,
        this.canonicalString,
        this.references,
        this.resolvedReferences,
        metadata,
        ruleSet,
        this.version,
        this.isNew
    );
  }

  @Override
  public ParsedSchema copy(Map<SchemaEntity, Set<String>> tagsToAdd,
                           Map<SchemaEntity, Set<String>> tagsToRemove) {
    AvroSchema schemaCopy = this.copy();
    JsonNode original;
    try {
      original = jsonMapper.readTree(schemaCopy.canonicalString());
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    modifySchemaTags(original, tagsToAdd, tagsToRemove);
    return new AvroSchema(original.toString(),
      schemaCopy.references(),
      schemaCopy.resolvedReferences(),
      schemaCopy.metadata(),
      schemaCopy.ruleSet(),
      schemaCopy.version(),
      schemaCopy.isNew());
  }

  protected Schema.Parser getParser() {
    Schema.Parser parser = new Schema.Parser();
    parser.setValidateDefaults(isNew());
    return parser;
  }

  @Override
  public Schema rawSchema() {
    return schemaObj;
  }

  @Override
  public String schemaType() {
    return TYPE;
  }

  @Override
  public String name() {
    if (schemaObj != null && schemaObj.getType() == Schema.Type.RECORD) {
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
      Schema.Parser parser = getParser();
      List<Schema> schemaRefs = new ArrayList<>();
      for (String schema : resolvedReferences.values()) {
        Schema schemaRef = parser.parse(schema);
        schemaRefs.add(schemaRef);
      }
      canonicalString = schemaObj.toString(schemaRefs, false);
    }
    return canonicalString;
  }

  @Override
  public String formattedString(String format) {
    if (format == null || format.trim().isEmpty()) {
      return canonicalString();
    }
    Format formatEnum = Format.get(format);
    switch (formatEnum) {
      case DEFAULT:
        return canonicalString();
      case RESOLVED:
        return schemaObj != null ? schemaObj.toString() : null;
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

  public boolean isNew() {
    return isNew;
  }

  @Override
  public AvroSchema normalize() {
    String normalized = AvroSchemaUtils.toNormalizedString(this);
    return new AvroSchema(
        normalized,
        this.references.stream().sorted().distinct().collect(Collectors.toList()),
        this.resolvedReferences,
        this.version,
        this.isNew
    );
  }

  @Override
  public List<String> isBackwardCompatible(ParsedSchema previousSchema) {
    if (!schemaType().equals(previousSchema.schemaType())) {
      return Lists.newArrayList("Incompatible because of different schema type");
    }
    try {
      SchemaCompatibility.SchemaPairCompatibility result =
          SchemaCompatibility.checkReaderWriterCompatibility(
              this.schemaObj,
              ((AvroSchema) previousSchema).schemaObj);
      return result.getResult().getIncompatibilities().stream()
          .map(Difference::new)
          .map(Difference::toString)
          .collect(Collectors.toCollection(ArrayList::new));
    } catch (Exception e) {
      log.error("Unexpected exception during compatibility check", e);
      return Lists.newArrayList(
              "Unexpected exception during compatibility check: " + e.getMessage());
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
    AvroSchema that = (AvroSchema) o;
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
      case RECORD:
        // Add a temporary value to the cache to avoid cycles.
        // As long as we recurse only at the end of the method, we can safely default to true here.
        // The cache is updated at the end of the method with the actual comparison result.
        IdentityPair<Schema, Schema> sp = new IdentityPair<>(schema1, schema2);
        Boolean cacheHit = cache.putIfAbsent(sp, true);
        if (cacheHit != null) {
          return cacheHit;
        }

        boolean equals = Objects.equals(schema1.getAliases(), schema2.getAliases())
            && Objects.equals(schema1.getDoc(), schema2.getDoc())
            && fieldMetaEqual(schema1.getFields(), schema2.getFields(), cache);

        cache.put(sp, equals);
        return equals;
      case ENUM:
        return Objects.equals(schema1.getAliases(), schema2.getAliases())
            && Objects.equals(schema1.getDoc(), schema2.getDoc())
            && Objects.equals(schema1.getEnumDefault(), schema2.getEnumDefault());
      case FIXED:
        return Objects.equals(schema1.getAliases(), schema2.getAliases())
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
      if (!Objects.equals(field1.aliases(), field2.aliases())
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
      case RECORD:
        // Add a temporary value to the cache to avoid cycles.
        // As long as we recurse only at the end of the method, we can safely default to 0 here.
        // The cache is updated at the end of the method with the actual comparison result.
        Integer cacheHit = cache.putIfAbsent(schema, 0);
        if (cacheHit != null) {
          return cacheHit;
        }

        int result = Objects.hash(schema.getAliases(), schema.getDoc())
            + fieldMetaHash(schema.getFields(), cache);

        cache.put(schema, result);
        return result;
      case ENUM:
        return Objects.hash(schema.getAliases(), schema.getDoc(), schema.getEnumDefault());
      case FIXED:
        return Objects.hash(schema.getAliases(), schema.getDoc());
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
      hash += Objects.hash(field.aliases(), field.doc()) + metaHash(field.schema(), cache);
    }
    return hash;
  }

  @Override
  public String toString() {
    return canonicalString();
  }

  @Override
  public Object fromJson(JsonNode json) throws IOException {
    return AvroSchemaUtils.toObject(json, this);
  }

  @Override
  public JsonNode toJson(Object message) throws IOException {
    if (message instanceof JsonNode) {
      return (JsonNode) message;
    }
    return JacksonMapper.INSTANCE.readTree(AvroSchemaUtils.toJson(message));
  }

  @Override
  public Object copyMessage(Object message) throws IOException {
    GenericData data = getData(message);
    return data.deepCopy(rawSchema(), message);
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
    FieldContext fieldCtx = ctx.currentField();
    if (schema == null) {
      return message;
    }
    if (fieldCtx != null) {
      fieldCtx.setType(getType(schema));
    }
    GenericData data;
    Schema.Type st = schema.getType();
    switch (st) {
      case UNION:
        data = getData(message);
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
        if (!(message instanceof Map)) {
          log.warn("Object does not match a map schema");
          return message;
        }
        return ((Map<?, ?>) message).entrySet().stream()
            .collect(Collectors.toMap(
                Entry::getKey,
                e -> toTransformedMessage(ctx, schema.getValueType(), e.getValue(), transform),
                (e1, e2) -> e1));
      case RECORD:
        if (message == null) {
          return message;
        }
        data = getData(message);
        for (Schema.Field f : schema.getFields()) {
          String fullName = schema.getFullName() + "." + f.name();
          try (FieldContext fc = ctx.enterField(
              message, fullName, f.name(), getType(f.schema()), getInlineTags(f))) {
            Object value = data.getField(message, f.name(), f.pos());
            if (value instanceof Utf8) {
              value = value.toString();
            }
            Object newValue = toTransformedMessage(ctx, f.schema(), value, transform);
            data.setField(message, f.name(), f.pos(), newValue);
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
              if (!Collections.disjoint(fieldCtx.getTags(), ruleTags)) {
                return fieldTransform(ctx, message, transform, fieldCtx);
              }
            }
          } catch (RuleException e) {
            throw new RuntimeException(e);
          }
        }
        return message;
    }
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

  private RuleContext.Type getType(Schema schema) {
    switch (schema.getType()) {
      case RECORD:
        return Type.RECORD;
      case ENUM:
        return Type.ENUM;
      case ARRAY:
        return Type.ARRAY;
      case MAP:
        return Type.MAP;
      case UNION:
        return Type.COMBINED;
      case FIXED:
        return Type.FIXED;
      case STRING:
        return Type.STRING;
      case BYTES:
        return Type.BYTES;
      case INT:
        return Type.INT;
      case LONG:
        return Type.LONG;
      case FLOAT:
        return Type.FLOAT;
      case DOUBLE:
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

  private Set<String> getInlineTags(Schema.Field field) {
    Object prop = field.getObjectProp(TAGS);
    if (prop instanceof List) {
      return ((List<?>)prop).stream().map(Object::toString).collect(Collectors.toSet());
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

  private static GenericData getData(Object message) {
    if (message instanceof SpecificRecord) {
      return SpecificData.get();
    } else if (message instanceof GenericRecord) {
      return GenericData.get();
    } else {
      return ReflectData.get();
    }
  }

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
    RESOLVED("resolved");

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
