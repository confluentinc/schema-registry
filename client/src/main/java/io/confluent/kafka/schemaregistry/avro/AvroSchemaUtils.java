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

package io.confluent.kafka.schemaregistry.avro;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Conversions;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectData.AllowNull;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.schemaregistry.utils.JacksonMapper;

import static io.confluent.kafka.schemaregistry.avro.AvroSchema.FIELDS_FIELD;
import static io.confluent.kafka.schemaregistry.avro.AvroSchema.NAME_FIELD;

public class AvroSchemaUtils {

  private static final GenericData GENERIC_DATA_INSTANCE = new GenericData();
  private static final ReflectData REFLECT_DATA_INSTANCE = new ReflectData();
  private static final ReflectData REFLECT_DATA_ALLOW_NULL_INSTANCE = new AllowNull();
  private static final SpecificData SPECIFIC_DATA_INSTANCE = new SpecificData();

  static {
    addLogicalTypeConversion(GENERIC_DATA_INSTANCE);
    addLogicalTypeConversion(REFLECT_DATA_INSTANCE);
    addLogicalTypeConversion(REFLECT_DATA_ALLOW_NULL_INSTANCE);
    addLogicalTypeConversion(SPECIFIC_DATA_INSTANCE);
  }

  public static GenericData getGenericData() {
    return GENERIC_DATA_INSTANCE;
  }

  public static ReflectData getReflectData() {
    return REFLECT_DATA_INSTANCE;
  }

  public static ReflectData getReflectDataAllowNull() {
    return REFLECT_DATA_ALLOW_NULL_INSTANCE;
  }

  public static SpecificData getSpecificData() {
    return SPECIFIC_DATA_INSTANCE;
  }

  public static SpecificData getSpecificDataForSchema(Schema reader) {
    if (reader != null && (reader.getType() == org.apache.avro.Schema.Type.RECORD
            || reader.getType() == org.apache.avro.Schema.Type.UNION)) {
      Class<?> clazz = getSpecificData().getClass(reader);
      if (clazz != null) {
        return getSpecificDataForClass(clazz);
      }
    }
    return getSpecificData();
  }

  public static <T> SpecificData getSpecificDataForClass(Class<T> c) {
    return SpecificRecordBase.class.isAssignableFrom(c)
            ? SpecificData.getForClass(c) : getSpecificData();
  }

  public static void addLogicalTypeConversion(GenericData avroData) {
    avroData.addLogicalTypeConversion(new Conversions.DecimalConversion());
    avroData.addLogicalTypeConversion(new Conversions.UUIDConversion());

    avroData.addLogicalTypeConversion(new TimeConversions.DateConversion());

    avroData.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
    avroData.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());

    avroData.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
    avroData.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());

    avroData.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
    avroData.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
  }

  private static final EncoderFactory encoderFactory = EncoderFactory.get();
  private static final DecoderFactory decoderFactory = DecoderFactory.get();
  private static final ObjectMapper jsonMapper = JacksonMapper.INSTANCE;
  private static final ObjectMapper jsonMapperWithOrderedProps =
      JsonMapper.builder()
          .nodeFactory(new SortingNodeFactory(false))
          .build();

  static class SortingNodeFactory extends JsonNodeFactory {
    public SortingNodeFactory(boolean bigDecimalExact) {
      super(bigDecimalExact);
    }

    @Override
    public ObjectNode objectNode() {
      return new ObjectNode(this, new TreeMap<>());
    }
  }

  private static int DEFAULT_CACHE_CAPACITY = 1000;
  private static final Map<String, Schema> primitiveSchemas;
  private static final Map<Schema, Schema> transformedSchemas =
      new BoundedConcurrentHashMap<>(DEFAULT_CACHE_CAPACITY);

  static {
    primitiveSchemas = new HashMap<>();
    primitiveSchemas.put("Null", createPrimitiveSchema("null"));
    primitiveSchemas.put("Boolean", createPrimitiveSchema("boolean"));
    primitiveSchemas.put("Integer", createPrimitiveSchema("int"));
    primitiveSchemas.put("Long", createPrimitiveSchema("long"));
    primitiveSchemas.put("Float", createPrimitiveSchema("float"));
    primitiveSchemas.put("Double", createPrimitiveSchema("double"));
    primitiveSchemas.put("String", createPrimitiveSchema("string"));
    primitiveSchemas.put("Bytes", createPrimitiveSchema("bytes"));
  }

  private static Schema createPrimitiveSchema(String type) {
    String schemaString = String.format("{\"type\" : \"%s\"}", type);
    return new AvroSchema(schemaString).rawSchema();
  }

  public static AvroSchema copyOf(AvroSchema schema) {
    return schema.copy();
  }

  public static Map<String, Schema> getPrimitiveSchemas() {
    return Collections.unmodifiableMap(primitiveSchemas);
  }

  public static Schema getSchema(Object object) {
    return getSchema(object, false, false, false, true);
  }

  public static Schema getSchema(Object object, boolean useReflection,
                                 boolean reflectionAllowNull, boolean removeJavaProperties) {
    return getSchema(object, useReflection, reflectionAllowNull, removeJavaProperties, true);
  }

  public static Schema getSchema(Object object, boolean useReflection,
                                 boolean reflectionAllowNull, boolean removeJavaProperties,
                                 boolean throwError) {
    return getSchema(object, useReflection, reflectionAllowNull, false,
        removeJavaProperties, throwError);
  }

  public static Schema getSchema(Object object, boolean useReflection,
                                 boolean reflectionAllowNull, boolean useLogicalTypeConverters,
                                 boolean removeJavaProperties, boolean throwError) {
    if (object == null) {
      return primitiveSchemas.get("Null");
    } else if (object instanceof Boolean) {
      return primitiveSchemas.get("Boolean");
    } else if (object instanceof Integer) {
      return primitiveSchemas.get("Integer");
    } else if (object instanceof Long) {
      return primitiveSchemas.get("Long");
    } else if (object instanceof Float) {
      return primitiveSchemas.get("Float");
    } else if (object instanceof Double) {
      return primitiveSchemas.get("Double");
    } else if (object instanceof CharSequence) {
      return primitiveSchemas.get("String");
    } else if (object instanceof byte[] || object instanceof ByteBuffer) {
      return primitiveSchemas.get("Bytes");
    } else if (useReflection) {
      ReflectData reflectData = reflectionAllowNull
          ? (useLogicalTypeConverters ? getReflectDataAllowNull() : ReflectData.AllowNull.get())
          : (useLogicalTypeConverters ? getReflectData() : ReflectData.get());
      Schema schema = reflectData.getSchema(object.getClass());
      if (schema == null) {
        throw new SerializationException("Schema is null for object of class " + object.getClass()
            .getCanonicalName());
      } else {
        return schema;
      }
    } else if (object instanceof GenericContainer) {
      Schema schema = ((GenericContainer) object).getSchema();
      if (removeJavaProperties) {
        final Schema s = schema;
        schema = transformedSchemas.computeIfAbsent(s, k -> removeJavaProperties(s));
      }
      return schema;
    } else if (object instanceof Map) {
      // This case is unusual -- the schema isn't available directly anywhere, instead we have to
      // take get the value schema out of one of the entries and then construct the full schema.
      Map mapValue = ((Map) object);
      if (mapValue.isEmpty()) {
        // In this case the value schema doesn't matter since there is no content anyway. This
        // only works because we know in this case that we are only using this for conversion and
        // no data will be added to the map.
        return Schema.createMap(primitiveSchemas.get("Null"));
      }
      Schema valueSchema = getSchema(mapValue.values().iterator().next(),
          useReflection, reflectionAllowNull, removeJavaProperties, throwError);
      return Schema.createMap(valueSchema);
    } else if (throwError) {
      throw new IllegalArgumentException(
          "Unsupported Avro type '" + object.getClass().getSimpleName()
              + "'. Supported types are null, Boolean, Integer, Long, "
              + "Float, Double, String, byte[] and IndexedRecord");

    } else {
      // Try reflection as last resort
      ReflectData reflectData = reflectionAllowNull
          ? (useLogicalTypeConverters ? getReflectDataAllowNull() : ReflectData.AllowNull.get())
          : (useLogicalTypeConverters ? getReflectData() : ReflectData.get());
      Schema schema = reflectData.getSchema(object.getClass());
      if (schema == null) {
        throw new SerializationException("Schema is null for object of class " + object.getClass()
            .getCanonicalName());
      } else {
        return schema;
      }
    }
  }

  private static Schema removeJavaProperties(Schema schema) {
    try {
      JsonNode node = jsonMapper.readTree(schema.toString());
      removeProperty(node, "avro.java.string");
      AvroSchema avroSchema = new AvroSchema(node.toString());
      return avroSchema.rawSchema();
    } catch (IOException e) {
      throw new SerializationException("Could not parse schema: " + schema.toString());
    }
  }

  private static void removeProperty(JsonNode node, String propertyName) {
    if (node.isObject()) {
      ObjectNode objectNode = (ObjectNode) node;
      objectNode.remove(propertyName);
      Iterator<JsonNode> elements = objectNode.elements();
      while (elements.hasNext()) {
        removeProperty(elements.next(), propertyName);
      }
    } else if (node.isArray()) {
      ArrayNode arrayNode = (ArrayNode) node;
      Iterator<JsonNode> elements = arrayNode.elements();
      while (elements.hasNext()) {
        removeProperty(elements.next(), propertyName);
      }
    }
  }

  public static Object toObject(JsonNode value, AvroSchema schema) throws IOException {
    return toObject(value, schema, new GenericDatumReader<>(
        schema.rawSchema(), schema.rawSchema(), getGenericData()));
  }

  public static Object toObject(
      JsonNode value, AvroSchema schema, DatumReader<Object> reader) throws IOException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      Schema rawSchema = schema.rawSchema();
      jsonMapper.writeValue(out, value);
      Object object = reader.read(null,
          decoderFactory.jsonDecoder(rawSchema, new ByteArrayInputStream(out.toByteArray()))
      );
      return object;
    }
  }

  public static Object toObject(String value, AvroSchema schema) throws IOException {
    return toObject(value, schema, new GenericDatumReader<>(
        schema.rawSchema(), schema.rawSchema(), getGenericData()));
  }

  public static Object toObject(
      String value, AvroSchema schema, DatumReader<Object> reader) throws IOException {
    Schema rawSchema = schema.rawSchema();
    Object object = reader.read(null,
        decoderFactory.jsonDecoder(rawSchema, value));
    return object;
  }

  public static byte[] toJson(Object value) throws IOException {
    if (value == null) {
      return null;
    }
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      toJson(value, out);
      return out.toByteArray();
    }
  }

  @SuppressWarnings("unchecked")
  public static void toJson(Object value, OutputStream out) throws IOException {
    Schema schema = getSchema(value, false, false, false, false);
    JsonEncoder encoder = encoderFactory.jsonEncoder(schema, out);
    DatumWriter<Object> writer = (DatumWriter<Object>) getDatumWriter(value, schema, true);
    // Some types require wrapping/conversion
    Object wrappedValue = value;
    if (value instanceof byte[]) {
      wrappedValue = ByteBuffer.wrap((byte[]) value);
    }
    writer.write(wrappedValue, encoder);
    encoder.flush();
  }

  public static DatumWriter<?> getDatumWriter(
      Object value, Schema schema, boolean avroUseLogicalTypeConverters) {
    if (value instanceof SpecificRecord) {
      return new SpecificDatumWriter<>(schema,
          avroUseLogicalTypeConverters
                  ? getSpecificDataForSchema(schema)
                  : SpecificData.getForSchema(schema));
    } else if (value instanceof GenericRecord) {
      return new GenericDatumWriter<>(schema,
          avroUseLogicalTypeConverters ? getGenericData() : GenericData.get());
    } else {
      return new ReflectDatumWriter<>(schema,
          avroUseLogicalTypeConverters ? getReflectData() : ReflectData.get());
    }
  }

  public static JsonNode findMatchingEntity(JsonNode node, SchemaEntity entity) {
    String[] identifiers = entity.getEntityPath().split("\\.");
    SchemaEntity.EntityType type = entity.getEntityType();
    String nameSpace;
    String recordName;
    String fieldName = null;

    if (SchemaEntity.EntityType.SR_RECORD == type) {
      nameSpace = String.join(".", Arrays.copyOfRange(identifiers, 0, identifiers.length - 1));
      recordName = identifiers[identifiers.length - 1];
    } else {
      nameSpace = String.join(".", Arrays.copyOfRange(identifiers, 0, identifiers.length - 2));
      recordName = identifiers[identifiers.length - 2];
      fieldName = identifiers[identifiers.length - 1];
    }

    LinkedList<JsonNodeWithNS> toVisit = new LinkedList<>();
    JsonNode currNameSpace = node.get("namespace");
    toVisit.add(new JsonNodeWithNS(node, currNameSpace == null ? null : currNameSpace.asText()));

    // BFS to search
    while (toVisit.size() > 0) {
      JsonNodeWithNS curr = toVisit.removeFirst();
      JsonNode currNode = curr.jsonNode();
      if (!currNode.has("type")) {
        // union
        currNode.elements().forEachRemaining(
            e -> toVisit.add(new JsonNodeWithNS(e, curr.namespace())));
      } else {
        String schemaType = currNode.get("type").asText();
        switch (schemaType) {
          case "record":
            if ((nameSpace.isEmpty() || nameSpace.equals(curr.namespace()))
                && recordName.equals(currNode.get(NAME_FIELD).asText())) {
              if (SchemaEntity.EntityType.SR_RECORD == type) {
                return currNode;
              } else {
                Iterator<JsonNode> fieldsIter = currNode.get(FIELDS_FIELD).elements();
                while (fieldsIter.hasNext()) {
                  JsonNode currField = fieldsIter.next();
                  if (fieldName.equals(currField.get(NAME_FIELD).asText())) {
                    return currField;
                  }
                }
              }
            } else {
              currNode.get(FIELDS_FIELD).elements()
                .forEachRemaining(e -> toVisit.add(new JsonNodeWithNS(e, curr.namespace())));
            }
            break;
          case "array":
            toVisit.add(new JsonNodeWithNS(currNode.get("items"), curr.namespace()));
            break;
          case "map":
            toVisit.add(new JsonNodeWithNS(currNode.get("values"), curr.namespace()));
            break;
          default:
            // nested type
            toVisit.add(new JsonNodeWithNS(currNode.get("type"), curr.namespace()));
        }
      }
    }
    throw new IllegalArgumentException(String.format(
      "No matching path '%s' found in the schema", entity.getEntityPath()));
  }

  protected static String toNormalizedString(AvroSchema schema) {
    try {
      Map<String, String> env = new HashMap<>();
      Schema.Parser parser = schema.getParser();
      for (String resolvedRef : schema.resolvedReferences().values()) {
        Schema schemaRef = parser.parse(resolvedRef);
        String fullName = schemaRef.getFullName();
        env.put(fullName, "\"" + fullName + "\"");
      }
      return build(env, schema.rawSchema(), new StringBuilder()).toString();
    } catch (IOException e) {
      // Shouldn't happen, b/c StringBuilder can't throw IOException
      throw new RuntimeException(e);
    }
  }

  // Adapted from SchemaNormalization.java in Avro
  private static Appendable build(Map<String, String> env, Schema s, Appendable o)
      throws IOException {
    boolean firstTime = true;
    Schema.Type st = s.getType();
    LogicalType lt = s.getLogicalType();
    switch (st) {
      case UNION:
        o.append('[');
        for (Schema b : s.getTypes()) {
          if (!firstTime) {
            o.append(',');
          } else {
            firstTime = false;
          }
          build(env, b, o);
        }
        return o.append(']');

      case ARRAY:
      case MAP:
        o.append("{\"type\":\"").append(st.getName()).append("\"");
        if (st == Schema.Type.ARRAY) {
          build(env, s.getElementType(), o.append(",\"items\":"));
        } else {
          build(env, s.getValueType(), o.append(",\"values\":"));
        }
        setSimpleProps(o, s.getObjectProps());
        return o.append("}");

      case ENUM:
      case FIXED:
      case RECORD:
        String name = s.getFullName();
        if (env.get(name) != null) {
          return o.append(env.get(name));
        }
        String qname = "\"" + name + "\"";
        env.put(name, qname);
        o.append("{\"name\":").append(qname);
        o.append(",\"type\":\"").append(st.getName()).append("\"");
        if (st == Schema.Type.ENUM) {
          o.append(",\"symbols\":[");
          for (String enumSymbol : s.getEnumSymbols()) {
            if (!firstTime) {
              o.append(',');
            } else {
              firstTime = false;
            }
            o.append('"').append(enumSymbol).append('"');
          }
          o.append("]");
        } else if (st == Schema.Type.FIXED) {
          o.append(",\"size\":").append(Integer.toString(s.getFixedSize()));
          lt = s.getLogicalType();
          // adding the logical property
          if (lt != null) {
            setLogicalProps(o, lt);
          }
        } else { // st == Schema.Type.RECORD
          o.append(",\"fields\":[");
          for (Schema.Field f : s.getFields()) {
            if (!firstTime) {
              o.append(',');
            } else {
              firstTime = false;
            }
            o.append("{\"name\":\"").append(f.name()).append("\"");
            build(env, f.schema(), o.append(",\"type\":"));
            setFieldProps(o, f);
            o.append("}");
          }
          o.append("]");
        }
        setComplexProps(o, s);
        setSimpleProps(o, s.getObjectProps());
        return o.append("}");

      default: // boolean, bytes, double, float, int, long, null, string
        if (lt != null) {
          return writeLogicalType(s, lt, o);
        } else {
          if (s.hasProps()) {
            o.append("{\"type\":\"").append(st.getName()).append('"');
            setSimpleProps(o, s.getObjectProps());
            o.append("}");
          } else {
            o.append('"').append(st.getName()).append('"');
          }
          return o;
        }
    }
  }

  private static Appendable writeLogicalType(Schema s, LogicalType lt, Appendable o)
      throws IOException {
    o.append("{\"type\":\"").append(s.getType().getName()).append("\"");
    // adding the logical property
    setLogicalProps(o, lt);
    // adding the reserved property
    setSimpleProps(o, s.getObjectProps());
    return o.append("}");
  }

  private static void setLogicalProps(Appendable o, LogicalType lt) throws IOException {
    o.append(",\"").append(LogicalType.LOGICAL_TYPE_PROP)
        .append("\":\"").append(lt.getName()).append("\"");
    if (lt.getName().equals("decimal")) {
      LogicalTypes.Decimal dlt = (LogicalTypes.Decimal) lt;
      o.append(",\"precision\":").append(Integer.toString(dlt.getPrecision()));
      if (dlt.getScale() != 0) {
        o.append(",\"scale\":").append(Integer.toString(dlt.getScale()));
      }
    }
  }

  private static void setSimpleProps(Appendable o, Map<String, Object> schemaProps)
      throws IOException {
    Map<String, Object> sortedProps = new TreeMap<>(schemaProps);
    for (Map.Entry<String, Object> entry : sortedProps.entrySet()) {
      String propKey = entry.getKey();
      String propValue = toJsonNode(entry.getValue()).toString();
      o.append(",\"").append(propKey).append("\":").append(propValue);
    }
  }

  private static void setComplexProps(Appendable o, Schema s) throws IOException {
    if (s.getDoc() != null && !s.getDoc().isEmpty()) {
      o.append(",\"doc\":").append(toJsonNode(s.getDoc()).toString());
    }
    Set<String> aliases = s.getAliases();
    if (!aliases.isEmpty()) {
      o.append(",\"aliases\":").append(toJsonNode(new TreeSet<>(aliases)).toString());
    }
    if (s.getType() == Schema.Type.ENUM && s.getEnumDefault() != null) {
      o.append(",\"default\":").append(toJsonNode(s.getEnumDefault()).toString());
    }
  }

  private static void setFieldProps(Appendable o, Schema.Field f) throws IOException {
    if (f.order() != null) {
      o.append(",\"order\":\"").append(f.order().toString()).append("\"");
    }
    if (f.doc() != null) {
      o.append(",\"doc\":").append(toJsonNode(f.doc()).toString());
    }
    Set<String> aliases = f.aliases();
    if (!aliases.isEmpty()) {
      o.append(",\"aliases\":").append(toJsonNode(new TreeSet<>(aliases)).toString());
    }
    if (f.defaultVal() != null) {
      o.append(",\"default\":").append(toJsonNode(f.defaultVal()).toString());
    }
    setSimpleProps(o, f.getObjectProps());
  }

  static JsonNode toJsonNode(Object datum) {
    if (datum == null) {
      return null;
    }
    try {
      TokenBuffer generator = new TokenBuffer(jsonMapperWithOrderedProps, false);
      genJson(datum, generator);
      return jsonMapperWithOrderedProps.readTree(generator.asParser());
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }

  @SuppressWarnings(value = "unchecked")
  static void genJson(Object datum, JsonGenerator generator) throws IOException {
    if (datum == JsonProperties.NULL_VALUE) { // null
      generator.writeNull();
    } else if (datum instanceof Map) { // record, map
      generator.writeStartObject();
      for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) datum).entrySet()) {
        generator.writeFieldName(entry.getKey().toString());
        genJson(entry.getValue(), generator);
      }
      generator.writeEndObject();
    } else if (datum instanceof Collection) { // array
      generator.writeStartArray();
      for (Object element : (Collection<?>) datum) {
        genJson(element, generator);
      }
      generator.writeEndArray();
    } else if (datum instanceof byte[]) { // bytes, fixed
      generator.writeString(new String((byte[]) datum, StandardCharsets.ISO_8859_1));
    } else if (datum instanceof CharSequence || datum instanceof Enum<?>) { // string, enum
      generator.writeString(datum.toString());
    } else if (datum instanceof Double) { // double
      generator.writeNumber((Double) datum);
    } else if (datum instanceof Float) { // float
      generator.writeNumber((Float) datum);
    } else if (datum instanceof Long) { // long
      generator.writeNumber((Long) datum);
    } else if (datum instanceof Integer) { // int
      generator.writeNumber((Integer) datum);
    } else if (datum instanceof Boolean) { // boolean
      generator.writeBoolean((Boolean) datum);
    } else if (datum instanceof BigInteger) {
      generator.writeNumber((BigInteger) datum);
    } else if (datum instanceof BigDecimal) {
      generator.writeNumber((BigDecimal) datum);
    } else {
      throw new AvroRuntimeException("Unknown datum class: " + datum.getClass());
    }
  }

  static class JsonNodeWithNS {
    private final JsonNode node;
    private final String namespace;

    public JsonNodeWithNS(JsonNode node, String parentNamespace) {
      this.node = node;

      JsonNode namespaceNode = node.get("namespace");
      this.namespace = namespaceNode == null ? parentNamespace : namespaceNode.asText();
    }

    public JsonNode jsonNode() {
      return node;
    }

    public String namespace() {
      return namespace;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      JsonNodeWithNS other = (JsonNodeWithNS) o;
      return Objects.equals(namespace, other.namespace()) && Objects.equals(node, other.jsonNode());
    }

    @Override
    public int hashCode() {
      int result = Objects.hashCode(node);
      result = 31 * result + Objects.hashCode(namespace);
      return result;
    }
  }
}
