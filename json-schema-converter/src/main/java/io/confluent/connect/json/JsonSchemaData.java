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

package io.confluent.connect.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.connect.schema.ConnectEnum;
import io.confluent.connect.schema.ConnectUnion;
import io.confluent.kafka.schemaregistry.json.jackson.Jackson;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ConstSchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.StringSchema;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.json.JSONArray;
import org.json.JSONObject;

import static io.confluent.connect.json.JsonSchemaDataConfig.SCHEMAS_CACHE_SIZE_CONFIG;
import static io.confluent.connect.json.JsonSchemaDataConfig.SCHEMAS_CACHE_SIZE_DEFAULT;

public class JsonSchemaData {

  public static final String NAMESPACE = "io.confluent.connect.json";

  public static final String KEY_FIELD = "key";
  public static final String VALUE_FIELD = "value";

  public static final String CONNECT_TYPE_PROP = "connect.type";
  public static final String CONNECT_VERSION_PROP = "connect.version";
  public static final String CONNECT_PARAMETERS_PROP = "connect.parameters";
  public static final String CONNECT_INDEX_PROP = "connect.index";

  public static final String CONNECT_TYPE_INT8 = "int8";
  public static final String CONNECT_TYPE_INT16 = "int16";
  public static final String CONNECT_TYPE_INT32 = "int32";
  public static final String CONNECT_TYPE_INT64 = "int64";
  public static final String CONNECT_TYPE_FLOAT32 = "float32";
  public static final String CONNECT_TYPE_FLOAT64 = "float64";
  public static final String CONNECT_TYPE_BYTES = "bytes";
  public static final String CONNECT_TYPE_MAP = "map";

  public static final String DEFAULT_ID_PREFIX = "#id";
  public static final String JSON_ID_PROP = NAMESPACE + ".Id";
  public static final String JSON_TYPE_ENUM = NAMESPACE + ".Enum";
  public static final String JSON_TYPE_ONE_OF = NAMESPACE + ".OneOf";

  public static final String GENERALIZED_TYPE_UNION = ConnectUnion.LOGICAL_PARAMETER;
  public static final String GENERALIZED_TYPE_ENUM = ConnectEnum.LOGICAL_PARAMETER;
  public static final String GENERALIZED_TYPE_UNION_PREFIX = "connect_union_";
  public static final String GENERALIZED_TYPE_UNION_FIELD_PREFIX =
      GENERALIZED_TYPE_UNION_PREFIX + "field_";

  public static final String NULL_MARKER = "<NULL>";

  private static final JsonNodeFactory JSON_NODE_FACTORY =
      JsonNodeFactory.withExactBigDecimals(true);

  private static final ObjectMapper OBJECT_MAPPER = Jackson.newObjectMapper();

  private static final Map<Schema.Type, JsonToConnectTypeConverter> TO_CONNECT_CONVERTERS =
      new EnumMap<>(
      Schema.Type.class);

  static {
    TO_CONNECT_CONVERTERS.put(Schema.Type.BOOLEAN, (data, schema, value) -> value.booleanValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.INT8, (data, schema, value) -> (byte) value.shortValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.INT16, (data, schema, value) -> value.shortValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.INT32, (data, schema, value) -> value.intValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.INT64, (data, schema, value) -> value.longValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT32, (data, schema, value) -> value.floatValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT64, (data, schema, value) -> value.doubleValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.BYTES, (data, schema, value) -> {
      try {
        Object o = value.binaryValue();
        if (o == null) {
          o = value.decimalValue();  // decimal logical type
        }
        return o;
      } catch (IOException e) {
        throw new DataException("Invalid bytes field", e);
      }
    });
    TO_CONNECT_CONVERTERS.put(Schema.Type.STRING, (data, schema, value) -> value.textValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.ARRAY, (data, schema, value) -> {
      Schema elemSchema = schema == null ? null : schema.valueSchema();
      ArrayList<Object> result = new ArrayList<>();
      for (JsonNode elem : value) {
        result.add(data.toConnectData(elemSchema, elem));
      }
      return result;
    });
    TO_CONNECT_CONVERTERS.put(Schema.Type.MAP, (data, schema, value) -> {
      Schema keySchema = schema == null ? null : schema.keySchema();
      Schema valueSchema = schema == null ? null : schema.valueSchema();

      Map<Object, Object> result = new HashMap<>();
      if (schema == null || (keySchema.type() == Schema.Type.STRING && !keySchema.isOptional())) {
        if (!value.isObject()) {
          throw new DataException(
              "Maps with string fields should be encoded as JSON objects, but found "
                  + value.getNodeType());
        }
        Iterator<Map.Entry<String, JsonNode>> fieldIt = value.fields();
        while (fieldIt.hasNext()) {
          Map.Entry<String, JsonNode> entry = fieldIt.next();
          result.put(entry.getKey(), data.toConnectData(valueSchema, entry.getValue()));
        }
      } else {
        if (!value.isArray()) {
          throw new DataException(
              "Maps with non-string fields should be encoded as JSON array of objects, but "
                  + "found "
                  + value.getNodeType());
        }
        for (JsonNode entry : value) {
          if (!entry.isObject()) {
            throw new DataException("Found invalid map entry instead of object: "
                + entry.getNodeType());
          }
          if (entry.size() != 2) {
            throw new DataException("Found invalid map entry, expected length 2 but found :" + entry
                .size());
          }
          result.put(data.toConnectData(keySchema, entry.get(KEY_FIELD)),
              data.toConnectData(valueSchema, entry.get(VALUE_FIELD))
          );
        }
      }
      return result;
    });
    TO_CONNECT_CONVERTERS.put(Schema.Type.STRUCT, (data, schema, value) -> {
      if (isUnionSchema(schema)) {
        boolean generalizedSumTypeSupport = ConnectUnion.isUnion(schema);
        String fieldNamePrefix = generalizedSumTypeSupport
            ? GENERALIZED_TYPE_UNION_FIELD_PREFIX
            : JSON_TYPE_ONE_OF + ".field.";
        int numMatchingProperties = -1;
        Field matchingField = null;
        for (Field field : schema.fields()) {
          Schema fieldSchema = field.schema();

          if (isInstanceOfSchemaTypeForSimpleSchema(fieldSchema, value)) {
            return new Struct(schema.schema()).put(fieldNamePrefix + field.index(),
                data.toConnectData(fieldSchema, value)
            );
          } else {
            int matching = matchStructSchema(fieldSchema, value);
            if (matching > numMatchingProperties) {
              numMatchingProperties = matching;
              matchingField = field;
            }
          }
        }
        if (matchingField != null) {
          return new Struct(schema.schema()).put(
              fieldNamePrefix + matchingField.index(),
              data.toConnectData(matchingField.schema(), value)
          );
        }
        throw new DataException("Did not find matching oneof field for data");
      } else {
        if (!value.isObject()) {
          throw new DataException("Structs should be encoded as JSON objects, but found "
              + value.getNodeType());
        }

        Struct result = new Struct(schema.schema());
        for (Field field : schema.fields()) {
          Object fieldValue = data.toConnectData(field.schema(), value.get(field.name()));
          if (fieldValue != null) {
            result.put(field, fieldValue);
          }
        }

        return result;
      }
    });
  }

  private static boolean isInstanceOfSchemaTypeForSimpleSchema(Schema fieldSchema, JsonNode value) {
    switch (fieldSchema.type()) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
        return value.isIntegralNumber();
      case FLOAT32:
      case FLOAT64:
        return value.isNumber();
      case BOOLEAN:
        return value.isBoolean();
      case STRING:
        return value.isTextual();
      case BYTES:
        return value.isBinary() || value.isBigDecimal();
      case ARRAY:
        return value.isArray();
      case MAP:
        return value.isObject() || value.isArray();
      case STRUCT:
        return false;
      default:
        throw new IllegalArgumentException("Unsupported type " + fieldSchema.type());
    }
  }

  private static int matchStructSchema(Schema fieldSchema, JsonNode value) {
    if (fieldSchema.type() != Schema.Type.STRUCT || !value.isObject()) {
      return -1;
    }
    Set<String> schemaFields = fieldSchema.fields()
        .stream()
        .map(Field::name)
        .collect(Collectors.toSet());
    Set<String> objectFields = new HashSet<>();
    for (Iterator<Entry<String, JsonNode>> iter = value.fields(); iter.hasNext(); ) {
      objectFields.add(iter.next().getKey());
    }
    Set<String> intersectSet = new HashSet<>(schemaFields);
    intersectSet.retainAll(objectFields);
    return intersectSet.size();
  }

  // Convert values in Kafka Connect form into their logical types. These logical converters are
  // discovered by logical type
  // names specified in the field
  private static final HashMap<String, JsonToConnectLogicalTypeConverter>
      TO_CONNECT_LOGICAL_CONVERTERS = new HashMap<>();

  static {
    TO_CONNECT_LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, (schema, value) -> {
      if (value.isNumber()) {
        return value.decimalValue();
      }
      if (value.isBinary() || value.isTextual()) {
        try {
          return Decimal.toLogical(schema, value.binaryValue());
        } catch (Exception e) {
          throw new DataException("Invalid bytes for Decimal field", e);
        }
      }

      throw new DataException("Invalid type for Decimal, "
          + "underlying representation should be numeric or bytes but was " + value.getNodeType());
    });

    TO_CONNECT_LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, (schema, value) -> {
      if (!(value.isInt())) {
        throw new DataException(
            "Invalid type for Date, "
            + "underlying representation should be integer but was " + value.getNodeType());
      }
      return Date.toLogical(schema, value.intValue());
    });

    TO_CONNECT_LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, (schema, value) -> {
      if (!(value.isInt())) {
        throw new DataException(
            "Invalid type for Time, "
            + "underlying representation should be integer but was " + value.getNodeType());
      }
      return Time.toLogical(schema, value.intValue());
    });

    TO_CONNECT_LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, (schema, value) -> {
      if (!(value.isIntegralNumber())) {
        throw new DataException(
            "Invalid type for Timestamp, "
            + "underlying representation should be integral but was " + value.getNodeType());
      }
      return Timestamp.toLogical(schema, value.longValue());
    });
  }

  private static final HashMap<String, ConnectToJsonLogicalTypeConverter>
      TO_JSON_LOGICAL_CONVERTERS = new HashMap<>();

  static {
    TO_JSON_LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, (schema, value, config) -> {
      if (!(value instanceof BigDecimal)) {
        throw new DataException("Invalid type for Decimal, "
            + "expected BigDecimal but was " + value.getClass());
      }

      final BigDecimal decimal = (BigDecimal) value;
      switch (config.decimalFormat()) {
        case NUMERIC:
          return JSON_NODE_FACTORY.numberNode(decimal);
        case BASE64:
          return JSON_NODE_FACTORY.binaryNode(Decimal.fromLogical(schema, decimal));
        default:
          throw new DataException("Unexpected "
              + JsonConverterConfig.DECIMAL_FORMAT_CONFIG + ": " + config.decimalFormat());
      }
    });

    TO_JSON_LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, (schema, value, config) -> {
      if (!(value instanceof java.util.Date)) {
        throw new DataException("Invalid type for Date, expected Date but was " + value.getClass());
      }
      return JSON_NODE_FACTORY.numberNode(Date.fromLogical(schema, (java.util.Date) value));
    });

    TO_JSON_LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, (schema, value, config) -> {
      if (!(value instanceof java.util.Date)) {
        throw new DataException("Invalid type for Time, expected Date but was " + value.getClass());
      }
      return JSON_NODE_FACTORY.numberNode(Time.fromLogical(schema, (java.util.Date) value));
    });

    TO_JSON_LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, (schema, value, config) -> {
      if (!(value instanceof java.util.Date)) {
        throw new DataException("Invalid type for Timestamp, "
            + "expected Date but was " + value.getClass());
      }
      return JSON_NODE_FACTORY.numberNode(
          Timestamp.fromLogical(schema, (java.util.Date) value));
    });
  }

  private final JsonSchemaDataConfig config;
  private final Map<Schema, JsonSchema> fromConnectSchemaCache;
  private final Map<JsonSchema, Schema> toConnectSchemaCache;
  private final boolean generalizedSumTypeSupport;

  public JsonSchemaData() {
    this(new JsonSchemaDataConfig.Builder().with(
        SCHEMAS_CACHE_SIZE_CONFIG,
        SCHEMAS_CACHE_SIZE_DEFAULT
    ).build());
  }

  public JsonSchemaData(JsonSchemaDataConfig jsonSchemaDataConfig) {
    this.config = jsonSchemaDataConfig;
    fromConnectSchemaCache = new BoundedConcurrentHashMap<>(jsonSchemaDataConfig.schemaCacheSize());
    toConnectSchemaCache = new BoundedConcurrentHashMap<>(jsonSchemaDataConfig.schemaCacheSize());
    generalizedSumTypeSupport = jsonSchemaDataConfig.isGeneralizedSumTypeSupport();
  }

  /**
   * Convert this object, in the org.apache.kafka.connect.data format, into a JSON object,
   * returning both the schema
   * and the converted object.
   */
  public JsonNode fromConnectData(Schema schema, Object logicalValue) {
    if (logicalValue == null) {
      if (schema == null) {
        // Any schema is valid and we don't have a default, so treat this as an optional schema
        return null;
      }
      if (schema.defaultValue() != null && !config.ignoreDefaultForNullables()) {
        return fromConnectData(schema, schema.defaultValue());
      }
      if (schema.isOptional()) {
        return JSON_NODE_FACTORY.nullNode();
      }
      return null;
    }

    Object value = logicalValue;
    if (schema != null && schema.name() != null) {
      ConnectToJsonLogicalTypeConverter logicalConverter =
          TO_JSON_LOGICAL_CONVERTERS.get(schema.name());
      if (logicalConverter != null) {
        return logicalConverter.convert(schema, logicalValue, config);
      }
    }

    try {
      final Schema.Type schemaType;
      if (schema == null) {
        schemaType = ConnectSchema.schemaType(value.getClass());
        if (schemaType == null) {
          throw new DataException("Java class "
              + value.getClass()
              + " does not have corresponding schema type.");
        }
      } else {
        schemaType = schema.type();
      }
      switch (schemaType) {
        case INT8:
          // Use shortValue to create a ShortNode, otherwise an IntNode will be created
          return JSON_NODE_FACTORY.numberNode(((Byte) value).shortValue());
        case INT16:
          return JSON_NODE_FACTORY.numberNode((Short) value);
        case INT32:
          return JSON_NODE_FACTORY.numberNode((Integer) value);
        case INT64:
          return JSON_NODE_FACTORY.numberNode((Long) value);
        case FLOAT32:
          return JSON_NODE_FACTORY.numberNode((Float) value);
        case FLOAT64:
          return JSON_NODE_FACTORY.numberNode((Double) value);
        case BOOLEAN:
          return JSON_NODE_FACTORY.booleanNode((Boolean) value);
        case STRING:
          CharSequence charSeq = (CharSequence) value;
          return JSON_NODE_FACTORY.textNode(charSeq.toString());
        case BYTES:
          if (value instanceof byte[]) {
            return JSON_NODE_FACTORY.binaryNode((byte[]) value);
          } else if (value instanceof ByteBuffer) {
            return JSON_NODE_FACTORY.binaryNode(((ByteBuffer) value).array());
          } else if (value instanceof BigDecimal) {
            return JSON_NODE_FACTORY.numberNode(((BigDecimal) value));
          } else {
            throw new DataException("Invalid type for bytes type: " + value.getClass());
          }
        case ARRAY: {
          Collection collection = (Collection) value;
          ArrayNode list = JSON_NODE_FACTORY.arrayNode();
          for (Object elem : collection) {
            Schema valueSchema = schema == null ? null : schema.valueSchema();
            JsonNode fieldValue = fromConnectData(valueSchema, elem);
            list.add(fieldValue);
          }
          return list;
        }
        case MAP: {
          Map<?, ?> map = (Map<?, ?>) value;
          // If true, using string keys and JSON object; if false, using non-string keys and
          // Array-encoding
          boolean objectMode;
          if (schema == null) {
            objectMode = true;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
              if (!(entry.getKey() instanceof String)) {
                objectMode = false;
                break;
              }
            }
          } else {
            objectMode = schema.keySchema().type() == Schema.Type.STRING && !schema.keySchema()
                .isOptional();
          }
          ObjectNode obj = null;
          ArrayNode list = null;
          if (objectMode) {
            obj = JSON_NODE_FACTORY.objectNode();
          } else {
            list = JSON_NODE_FACTORY.arrayNode();
          }
          for (Map.Entry<?, ?> entry : map.entrySet()) {
            Schema keySchema = schema == null ? null : schema.keySchema();
            Schema valueSchema = schema == null ? null : schema.valueSchema();
            JsonNode mapKey = fromConnectData(keySchema, entry.getKey());
            JsonNode mapValue = fromConnectData(valueSchema, entry.getValue());

            if (objectMode) {
              obj.set(mapKey.asText(), mapValue);
            } else {
              ObjectNode o = JSON_NODE_FACTORY.objectNode();
              o.set(KEY_FIELD, mapKey);
              o.set(VALUE_FIELD, mapValue);
              list.add(o);
            }
          }
          return objectMode ? obj : list;
        }
        case STRUCT: {
          Struct struct = (Struct) value;
          if (!struct.schema().equals(schema)) {
            throw new DataException("Mismatching schema.");
          }
          //This handles the inverting of a union which is held as a struct, where each field is
          // one of the union types.
          if (isUnionSchema(schema)) {
            for (Field field : schema.fields()) {
              Object object = config.ignoreDefaultForNullables()
                  ? struct.getWithoutDefault(field.name()) : struct.get(field);
              if (object != null) {
                return fromConnectData(field.schema(), object);
              }
            }
            return fromConnectData(schema, null);
          } else {
            ObjectNode obj = JSON_NODE_FACTORY.objectNode();
            for (Field field : schema.fields()) {
              Object fieldValue = config.ignoreDefaultForNullables()
                  ? struct.getWithoutDefault(field.name()) : struct.get(field);
              JsonNode jsonNode = fromConnectData(field.schema(), fieldValue);
              if (jsonNode != null) {
                obj.set(field.name(), jsonNode);
              }
            }
            return obj;
          }
        }
        default:
          break;
      }

      throw new DataException("Couldn't convert value to JSON.");
    } catch (ClassCastException e) {
      String schemaTypeStr = (schema != null) ? schema.type().toString() : "unknown schema";
      throw new DataException("Invalid type for " + schemaTypeStr + ": " + value.getClass());
    }
  }

  public Object toConnectData(Schema schema, JsonNode jsonValue) {
    final Schema.Type schemaType;
    if (schema != null) {
      schemaType = schema.type();
      if (jsonValue == null || jsonValue.isNull()) {
        if (schema.defaultValue() != null) {
          // any logical type conversions should already have been applied
          return schema.defaultValue();
        }
        if (jsonValue == null || schema.isOptional()) {
          return null;
        }
        throw new DataException("Invalid null value for required " + schemaType + " field");
      }
    } else {
      if (jsonValue == null) {
        return null;
      }
      switch (jsonValue.getNodeType()) {
        case NULL:
          return null;
        case BOOLEAN:
          schemaType = Schema.Type.BOOLEAN;
          break;
        case NUMBER:
          if (jsonValue.isIntegralNumber()) {
            schemaType = Schema.Type.INT64;
          } else {
            schemaType = Schema.Type.FLOAT64;
          }
          break;
        case ARRAY:
          schemaType = Schema.Type.ARRAY;
          break;
        case OBJECT:
          schemaType = Schema.Type.MAP;
          break;
        case STRING:
          schemaType = Schema.Type.STRING;
          break;

        case BINARY:
        case MISSING:
        case POJO:
        default:
          schemaType = null;
          break;
      }
    }

    final JsonToConnectTypeConverter typeConverter = TO_CONNECT_CONVERTERS.get(schemaType);
    if (typeConverter == null) {
      throw new DataException("Unknown schema type: " + schemaType);
    }

    if (schema != null && schema.name() != null) {
      JsonToConnectLogicalTypeConverter logicalConverter =
          TO_CONNECT_LOGICAL_CONVERTERS.get(schema.name());
      if (logicalConverter != null) {
        return logicalConverter.convert(schema, jsonValue);
      }
    }
    return typeConverter.convert(this, schema, jsonValue);
  }

  public JsonSchema fromConnectSchema(Schema schema) {
    if (schema == null) {
      return null;
    }
    JsonSchema cachedSchema = fromConnectSchemaCache.get(schema);
    if (cachedSchema != null) {
      return cachedSchema;
    }
    FromConnectContext ctx = new FromConnectContext();
    JsonSchema resultSchema = new JsonSchema(rawSchemaFromConnectSchema(ctx, schema));
    fromConnectSchemaCache.put(schema, resultSchema);
    return resultSchema;
  }

  private org.everit.json.schema.Schema rawSchemaFromConnectSchema(
      FromConnectContext ctx, Schema schema) {
    return rawSchemaFromConnectSchema(ctx, schema, null);
  }

  private org.everit.json.schema.Schema rawSchemaFromConnectSchema(
      FromConnectContext ctx, Schema schema, Integer index) {
    return rawSchemaFromConnectSchema(ctx, schema, index, false);
  }

  private org.everit.json.schema.Schema rawSchemaFromConnectSchema(
      FromConnectContext ctx, Schema schema, Integer index, boolean ignoreOptional
  ) {
    if (schema == null) {
      return null;
    }

    String id = null;
    if (schema.parameters() != null && schema.parameters().containsKey(JSON_ID_PROP)) {
      id = schema.parameters().get(JSON_ID_PROP);
      ctx.add(id);
    }

    org.everit.json.schema.Schema.Builder builder;
    Map<String, Object> unprocessedProps = new HashMap<>();
    switch (schema.type()) {
      case INT8:
        builder = NumberSchema.builder().requiresInteger(true);
        unprocessedProps.put(CONNECT_TYPE_PROP, CONNECT_TYPE_INT8);
        break;
      case INT16:
        builder = NumberSchema.builder().requiresInteger(true);
        unprocessedProps.put(CONNECT_TYPE_PROP, CONNECT_TYPE_INT16);
        break;
      case INT32:
        builder = NumberSchema.builder().requiresInteger(true);
        unprocessedProps.put(CONNECT_TYPE_PROP, CONNECT_TYPE_INT32);
        break;
      case INT64:
        builder = NumberSchema.builder().requiresInteger(true);
        unprocessedProps.put(CONNECT_TYPE_PROP, CONNECT_TYPE_INT64);
        break;
      case FLOAT32:
        builder = NumberSchema.builder().requiresInteger(false);
        unprocessedProps.put(CONNECT_TYPE_PROP, CONNECT_TYPE_FLOAT32);
        break;
      case FLOAT64:
        builder = NumberSchema.builder().requiresInteger(false);
        unprocessedProps.put(CONNECT_TYPE_PROP, CONNECT_TYPE_FLOAT64);
        break;
      case BOOLEAN:
        builder = BooleanSchema.builder();
        break;
      case STRING:
        if (schema.parameters() != null
            && (schema.parameters().containsKey(GENERALIZED_TYPE_ENUM)
                || schema.parameters().containsKey(JSON_TYPE_ENUM))) {
          EnumSchema.Builder enumBuilder = EnumSchema.builder();
          String paramName = generalizedSumTypeSupport ? GENERALIZED_TYPE_ENUM : JSON_TYPE_ENUM;
          for (Map.Entry<String, String> entry : schema.parameters().entrySet()) {
            if (entry.getKey().startsWith(paramName + ".")) {
              String enumSymbol = entry.getKey().substring(paramName.length() + 1);
              if (enumSymbol.equals(NULL_MARKER)) {
                enumSymbol = null;
              }
              enumBuilder.possibleValue(enumSymbol);
            }
          }
          builder = enumBuilder;
        } else {
          builder = StringSchema.builder();
        }
        break;
      case BYTES:
        builder = Decimal.LOGICAL_NAME.equals(schema.name())
                  ? NumberSchema.builder()
                  : StringSchema.builder();
        unprocessedProps.put(CONNECT_TYPE_PROP, CONNECT_TYPE_BYTES);
        break;
      case ARRAY:
        Schema arrayValueSchema = schema.valueSchema();
        String refId = null;
        if (arrayValueSchema.parameters() != null
            && arrayValueSchema.parameters().containsKey(JSON_ID_PROP)) {
          refId = arrayValueSchema.parameters().get(JSON_ID_PROP);
        }
        org.everit.json.schema.Schema itemsSchema;
        if (ctx.contains(refId)) {
          itemsSchema = ReferenceSchema.builder().refValue(refId).build();
        } else {
          itemsSchema = rawSchemaFromConnectSchema(ctx, arrayValueSchema);
        }
        builder = ArraySchema.builder().allItemSchema(itemsSchema);
        break;
      case MAP:
        // JSON Schema only supports string keys
        if (schema.keySchema().type() == Schema.Type.STRING && !schema.keySchema().isOptional()) {
          org.everit.json.schema.Schema valueSchema =
              rawSchemaFromConnectSchema(ctx, schema.valueSchema());
          builder = ObjectSchema.builder().schemaOfAdditionalProperties(valueSchema);
          unprocessedProps.put(CONNECT_TYPE_PROP, CONNECT_TYPE_MAP);
        } else {
          ObjectSchema.Builder entryBuilder = ObjectSchema.builder();
          org.everit.json.schema.Schema keySchema =
              rawSchemaFromConnectSchema(ctx, schema.keySchema(), 0);
          org.everit.json.schema.Schema valueSchema =
              rawSchemaFromConnectSchema(ctx, schema.valueSchema(), 1);
          entryBuilder.addPropertySchema(KEY_FIELD, keySchema);
          entryBuilder.addPropertySchema(VALUE_FIELD, valueSchema);
          builder = ArraySchema.builder().allItemSchema(entryBuilder.build());
          unprocessedProps.put(CONNECT_TYPE_PROP, CONNECT_TYPE_MAP);
        }
        break;
      case STRUCT:
        if (isUnionSchema(schema)) {
          CombinedSchema.Builder combinedBuilder = CombinedSchema.builder();
          combinedBuilder.criterion(CombinedSchema.ONE_CRITERION);
          if (schema.isOptional()) {
            combinedBuilder.subschema(NullSchema.INSTANCE);
          }
          for (Field field : schema.fields()) {
            combinedBuilder.subschema(rawSchemaFromConnectSchema(ctx, nonOptional(field.schema()),
                field.index(),
                true
            ));
          }
          builder = combinedBuilder;
        } else if (schema.isOptional()) {
          CombinedSchema.Builder combinedBuilder = CombinedSchema.builder();
          combinedBuilder.criterion(CombinedSchema.ONE_CRITERION);
          combinedBuilder.subschema(NullSchema.INSTANCE);
          combinedBuilder.subschema(rawSchemaFromConnectSchema(ctx, nonOptional(schema)));
          builder = combinedBuilder;
        } else {
          ObjectSchema.Builder objectBuilder = ObjectSchema.builder();
          for (Field field : schema.fields()) {
            Schema fieldSchema = field.schema();
            String fieldRefId = null;
            if (fieldSchema.parameters() != null
                && fieldSchema.parameters().containsKey(JSON_ID_PROP)) {
              fieldRefId = fieldSchema.parameters().get(JSON_ID_PROP);
            }
            org.everit.json.schema.Schema jsonSchema;
            if (ctx.contains(fieldRefId)) {
              jsonSchema = ReferenceSchema.builder().refValue(fieldRefId).build();
            } else {
              jsonSchema = rawSchemaFromConnectSchema(ctx, fieldSchema, field.index());
            }
            objectBuilder.addPropertySchema(field.name(), jsonSchema);
          }
          if (!config.allowAdditionalProperties()) {
            objectBuilder.additionalProperties(false);
          }
          builder = objectBuilder;
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported type " + schema.type());
    }

    if (!(builder instanceof CombinedSchema.Builder)) {
      if (schema.name() != null) {
        builder.title(schema.name());
      }
      if (schema.version() != null) {
        unprocessedProps.put(CONNECT_VERSION_PROP, schema.version());
      }
      if (schema.doc() != null) {
        builder.description(schema.doc());
      }
      if (schema.parameters() != null) {
        Map<String, String> parameters = schema.parameters()
            .entrySet()
            .stream()
            .filter(e -> !e.getKey().startsWith(NAMESPACE))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (parameters.size() > 0) {
          unprocessedProps.put(CONNECT_PARAMETERS_PROP, parameters);
        }
      }
      if (schema.defaultValue() != null) {
        builder.defaultValue(toJsonSchemaValue(fromConnectData(schema, schema.defaultValue())));
      }

      if (!ignoreOptional) {
        if (schema.isOptional()) {
          CombinedSchema.Builder combinedBuilder = CombinedSchema.builder();
          combinedBuilder.criterion(CombinedSchema.ONE_CRITERION);
          combinedBuilder.subschema(NullSchema.INSTANCE);
          combinedBuilder.subschema(builder.unprocessedProperties(unprocessedProps).build());
          if (index != null) {
            combinedBuilder.unprocessedProperties(Collections.singletonMap(CONNECT_INDEX_PROP,
                index
            ));
          }
          builder = combinedBuilder;
          unprocessedProps = new HashMap<>();
        }
      }
    }
    if (id != null) {
      builder.id(id);
    }
    if (index != null) {
      unprocessedProps.put(CONNECT_INDEX_PROP, index);
    }
    return builder.unprocessedProperties(unprocessedProps).build();
  }

  private static final Object NONE_MARKER = new Object();

  private static Object toJsonSchemaValue(Object value) {
    try {
      Object primitiveValue = NONE_MARKER;
      if (value instanceof BinaryNode) {
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
        return primitiveValue;
      } else {
        Object jsonObject;
        if (value instanceof ArrayNode) {
          jsonObject = OBJECT_MAPPER.treeToValue(((ArrayNode) value), JSONArray.class);
        } else if (value instanceof JsonNode) {
          jsonObject = OBJECT_MAPPER.treeToValue(((JsonNode) value), JSONObject.class);
        } else if (value.getClass().isArray()) {
          jsonObject = OBJECT_MAPPER.convertValue(value, JSONArray.class);
        } else {
          jsonObject = OBJECT_MAPPER.convertValue(value, JSONObject.class);
        }
        return jsonObject;
      }
    } catch (JsonProcessingException e) {
      throw new DataException("Invalid default value", e);
    }
  }


  private static Schema nonOptional(Schema schema) {
    return new ConnectSchema(schema.type(),
        false,
        schema.defaultValue(),
        schema.name(),
        schema.version(),
        schema.doc(),
        schema.parameters(),
        fields(schema),
        keySchema(schema),
        valueSchema(schema)
    );
  }

  private static List<Field> fields(Schema schema) {
    Schema.Type type = schema.type();
    if (Schema.Type.STRUCT.equals(type)) {
      return schema.fields();
    } else {
      return null;
    }
  }

  private static Schema keySchema(Schema schema) {
    Schema.Type type = schema.type();
    if (Schema.Type.MAP.equals(type)) {
      return schema.keySchema();
    } else {
      return null;
    }
  }

  private static Schema valueSchema(Schema schema) {
    Schema.Type type = schema.type();
    if (Schema.Type.MAP.equals(type) || Schema.Type.ARRAY.equals(type)) {
      return schema.valueSchema();
    } else {
      return null;
    }
  }

  public Schema toConnectSchema(JsonSchema schema) {
    if (schema == null) {
      return null;
    }
    if (config.ignoreModernDialects()) {
      schema = schema.copyIgnoringModernDialects();
    }
    Schema cachedSchema = toConnectSchemaCache.get(schema);
    if (cachedSchema != null) {
      return cachedSchema;
    }
    ToConnectContext ctx = new ToConnectContext();
    Schema resultSchema = toConnectSchema(ctx, schema.rawSchema(), schema.version(), false);
    toConnectSchemaCache.put(schema, resultSchema);
    return resultSchema;
  }

  @VisibleForTesting
  protected Schema toConnectSchema(org.everit.json.schema.Schema jsonSchema) {
    ToConnectContext ctx = new ToConnectContext();
    return toConnectSchema(ctx, jsonSchema, null);
  }

  private Schema toConnectSchema(ToConnectContext ctx, org.everit.json.schema.Schema jsonSchema) {
    return toConnectSchema(ctx, jsonSchema, null);
  }

  private Schema toConnectSchema(
      ToConnectContext ctx, org.everit.json.schema.Schema jsonSchema, Integer version) {
    return toConnectSchema(ctx, jsonSchema, version, false);
  }

  private Schema toConnectSchema(
      ToConnectContext ctx, org.everit.json.schema.Schema jsonSchema,
      Integer version, boolean forceOptional
  ) {
    if (jsonSchema == null) {
      return null;
    }

    final SchemaBuilder builder;
    if (jsonSchema instanceof BooleanSchema) {
      builder = SchemaBuilder.bool();
    } else if (jsonSchema instanceof NumberSchema) {
      NumberSchema numberSchema = (NumberSchema) jsonSchema;
      String type = (String) numberSchema.getUnprocessedProperties().get(CONNECT_TYPE_PROP);
      if (type == null) {
        builder = numberSchema.requiresInteger() ? SchemaBuilder.int64() : SchemaBuilder.float64();
      } else {
        switch (type) {
          case CONNECT_TYPE_INT8:
            builder = SchemaBuilder.int8();
            break;
          case CONNECT_TYPE_INT16:
            builder = SchemaBuilder.int16();
            break;
          case CONNECT_TYPE_INT32:
            builder = SchemaBuilder.int32();
            break;
          case CONNECT_TYPE_INT64:
            builder = SchemaBuilder.int64();
            break;
          case CONNECT_TYPE_FLOAT32:
            builder = SchemaBuilder.float32();
            break;
          case CONNECT_TYPE_FLOAT64:
            builder = SchemaBuilder.float64();
            break;
          case CONNECT_TYPE_BYTES:  // decimal logical type
            builder = SchemaBuilder.bytes();
            break;
          default:
            throw new IllegalArgumentException("Unsupported type " + type);
        }
      }
    } else if (jsonSchema instanceof StringSchema) {
      String type = (String) jsonSchema.getUnprocessedProperties().get(CONNECT_TYPE_PROP);
      builder = CONNECT_TYPE_BYTES.equals(type) ? SchemaBuilder.bytes() : SchemaBuilder.string();
    } else if (jsonSchema instanceof EnumSchema) {
      EnumSchema enumSchema = (EnumSchema) jsonSchema;
      builder = SchemaBuilder.string();
      String paramName = generalizedSumTypeSupport ? GENERALIZED_TYPE_ENUM : JSON_TYPE_ENUM;
      builder.parameter(paramName, "");  // JSON enums have no name, use empty string as placeholder
      int symbolIndex = 0;
      for (Object enumObj : enumSchema.getPossibleValuesAsList()) {
        String enumSymbol = enumObj != null ? enumObj.toString() : NULL_MARKER;
        if (generalizedSumTypeSupport) {
          builder.parameter(paramName + "." + enumSymbol, String.valueOf(symbolIndex));
        } else {
          builder.parameter(paramName + "." + enumSymbol, enumSymbol);
        }
        symbolIndex++;
      }
    } else if (jsonSchema instanceof CombinedSchema) {
      CombinedSchema combinedSchema = (CombinedSchema) jsonSchema;
      CombinedSchema.ValidationCriterion criterion = combinedSchema.getCriterion();
      String name;
      if (criterion == CombinedSchema.ONE_CRITERION || criterion == CombinedSchema.ANY_CRITERION) {
        if (generalizedSumTypeSupport) {
          name = GENERALIZED_TYPE_UNION_PREFIX + ctx.getAndIncrementUnionIndex();
        } else {
          name = JSON_TYPE_ONE_OF;
        }
      } else if (criterion == CombinedSchema.ALL_CRITERION) {
        return allOfToConnectSchema(ctx, combinedSchema, version, forceOptional);
      } else {
        throw new IllegalArgumentException("Unsupported criterion: " + criterion);
      }
      if (combinedSchema.getSubschemas().size() == 2) {
        boolean foundNullSchema = false;
        org.everit.json.schema.Schema nonNullSchema = null;
        for (org.everit.json.schema.Schema subSchema : combinedSchema.getSubschemas()) {
          if (subSchema instanceof NullSchema) {
            foundNullSchema = true;
          } else {
            nonNullSchema = subSchema;
          }
        }
        if (foundNullSchema) {
          return toConnectSchema(ctx, nonNullSchema, version, true);
        }
      }
      int index = 0;
      builder = SchemaBuilder.struct().name(name);
      if (generalizedSumTypeSupport) {
        builder.parameter(GENERALIZED_TYPE_UNION, name);
      }
      for (org.everit.json.schema.Schema subSchema : combinedSchema.getSubschemas()) {
        if (subSchema instanceof NullSchema) {
          builder.optional();
        } else {
          String subFieldName = generalizedSumTypeSupport
              ? GENERALIZED_TYPE_UNION_FIELD_PREFIX + index
              : name + ".field." + index;
          builder.field(subFieldName, toConnectSchema(ctx, subSchema, null, true));
          index++;
        }
      }
    } else if (jsonSchema instanceof ArraySchema) {
      ArraySchema arraySchema = (ArraySchema) jsonSchema;
      org.everit.json.schema.Schema itemsSchema = arraySchema.getAllItemSchema();
      if (itemsSchema == null) {
        throw new DataException("Array schema did not specify the items type");
      }
      String type = (String) arraySchema.getUnprocessedProperties().get(CONNECT_TYPE_PROP);
      if (CONNECT_TYPE_MAP.equals(type) && itemsSchema instanceof ObjectSchema) {
        ObjectSchema objectSchema = (ObjectSchema) itemsSchema;
        builder = SchemaBuilder.map(toConnectSchema(ctx, objectSchema.getPropertySchemas()
                .get(KEY_FIELD)),
            toConnectSchema(ctx, objectSchema.getPropertySchemas().get(VALUE_FIELD))
        );
      } else {
        builder = SchemaBuilder.array(toConnectSchema(ctx, itemsSchema));
      }
    } else if (jsonSchema instanceof ObjectSchema) {
      ObjectSchema objectSchema = (ObjectSchema) jsonSchema;
      String type = (String) objectSchema.getUnprocessedProperties().get(CONNECT_TYPE_PROP);
      if (CONNECT_TYPE_MAP.equals(type)) {
        builder = SchemaBuilder.map(Schema.STRING_SCHEMA,
            toConnectSchema(ctx, objectSchema.getSchemaOfAdditionalProperties())
        );
      } else {
        builder = SchemaBuilder.struct();
        ctx.put(objectSchema, builder);
        Map<String, org.everit.json.schema.Schema> properties = objectSchema.getPropertySchemas();
        SortedMap<Integer, Map.Entry<String, org.everit.json.schema.Schema>> sortedMap =
            new TreeMap<>();
        for (Map.Entry<String, org.everit.json.schema.Schema> property : properties.entrySet()) {
          org.everit.json.schema.Schema subSchema = property.getValue();
          Integer index = (Integer) subSchema.getUnprocessedProperties().get(CONNECT_INDEX_PROP);
          if (index == null) {
            index = sortedMap.size();
          }
          sortedMap.put(index, property);
        }
        for (Map.Entry<String, org.everit.json.schema.Schema> property : sortedMap.values()) {
          String subFieldName = property.getKey();
          org.everit.json.schema.Schema subSchema = property.getValue();
          boolean isFieldOptional = config.useOptionalForNonRequiredProperties()
              && !objectSchema.getRequiredProperties().contains(subFieldName);
          builder.field(subFieldName, toConnectSchema(ctx, subSchema, null, isFieldOptional));
        }
      }
    } else if (jsonSchema instanceof ReferenceSchema) {
      ReferenceSchema refSchema = (ReferenceSchema) jsonSchema;
      SchemaBuilder refBuilder = ctx.get(refSchema.getReferredSchema());
      if (refBuilder != null) {
        refBuilder.parameter(JSON_ID_PROP, DEFAULT_ID_PREFIX + ctx.incrementAndGetIdIndex());
        return new SchemaWrapper(refBuilder, forceOptional);
      } else {
        return toConnectSchema(ctx, refSchema.getReferredSchema(), version, forceOptional);
      }
    } else {
      throw new DataException("Unsupported schema type " + jsonSchema.getClass().getName());
    }

    String title = jsonSchema.getTitle();
    if (title != null && builder.name() == null) {
      builder.name(title);
    }
    // Included Kafka Connect version takes priority, fall back to schema registry version
    Integer connectVersion = (Integer) jsonSchema.getUnprocessedProperties()
        .get(CONNECT_VERSION_PROP);
    if (connectVersion != null) {
      builder.version(connectVersion);
    } else if (version != null) {
      builder.version(version);
    }
    String description = jsonSchema.getDescription();
    if (description != null) {
      builder.doc(description);
    }
    Map<String, String> parameters = (Map<String, String>) jsonSchema.getUnprocessedProperties()
        .get(CONNECT_PARAMETERS_PROP);
    if (parameters != null) {
      builder.parameters(parameters);
    }
    if (jsonSchema.hasDefaultValue()) {
      Object defaultVal = jsonSchema.getDefaultValue();
      JsonNode jsonNode = defaultVal == JSONObject.NULL
          ? NullNode.getInstance()
          : OBJECT_MAPPER.convertValue(defaultVal, JsonNode.class);
      builder.defaultValue(toConnectData(builder, jsonNode));
    }

    if (forceOptional) {
      builder.optional();
    }

    Schema result = builder.build();
    return result;
  }

  private Schema allOfToConnectSchema(
      ToConnectContext ctx, CombinedSchema combinedSchema,
      Integer version, boolean forceOptional) {
    ConstSchema constSchema = null;
    EnumSchema enumSchema = null;
    NumberSchema numberSchema = null;
    StringSchema stringSchema = null;
    CombinedSchema combinedSubschema = null;
    ReferenceSchema referenceSchema = null;
    Map<String, org.everit.json.schema.Schema> properties = new LinkedHashMap<>();
    Map<String, Boolean> required = new HashMap<>();
    for (org.everit.json.schema.Schema subSchema : combinedSchema.getSubschemas()) {
      if (subSchema instanceof ConstSchema) {
        constSchema = (ConstSchema) subSchema;
      } else if (subSchema instanceof EnumSchema) {
        enumSchema = (EnumSchema) subSchema;
      } else if (subSchema instanceof NumberSchema) {
        numberSchema = (NumberSchema) subSchema;
      } else if (subSchema instanceof StringSchema) {
        stringSchema = (StringSchema) subSchema;
      } else if (subSchema instanceof CombinedSchema) {
        combinedSubschema = (CombinedSchema) subSchema;
      } else if (subSchema instanceof ReferenceSchema) {
        referenceSchema = (ReferenceSchema) subSchema;
      }
      collectPropertySchemas(subSchema, properties, required, new HashSet<>());
    }
    if (!properties.isEmpty()) {
      SchemaBuilder builder = SchemaBuilder.struct();
      ctx.put(combinedSchema, builder);
      for (Map.Entry<String, org.everit.json.schema.Schema> property : properties.entrySet()) {
        String subFieldName = property.getKey();
        org.everit.json.schema.Schema subSchema = property.getValue();
        boolean isFieldOptional = config.useOptionalForNonRequiredProperties()
            && !required.get(subFieldName);
        builder.field(subFieldName, toConnectSchema(ctx, subSchema, null, isFieldOptional));
      }
      if (forceOptional) {
        builder.optional();
      }
      return builder.build();
    } else if (combinedSubschema != null) {
      // Any combined subschema takes precedence over primitive subschemas
      return toConnectSchema(ctx, combinedSubschema, version, forceOptional);
    } else if (constSchema != null) {
      if (stringSchema != null) {
        // Ignore the const, return the string
        return toConnectSchema(ctx, stringSchema, version, forceOptional);
      } else if (numberSchema != null) {
        // Ignore the const, return the number or integer
        return toConnectSchema(ctx, numberSchema, version, forceOptional);
      }
    } else if (enumSchema != null) {
      if (stringSchema != null) {
        // Return a string enum
        return toConnectSchema(ctx, enumSchema, version, forceOptional);
      } else if (numberSchema != null) {
        // Ignore the enum, return the number or integer
        return toConnectSchema(ctx, numberSchema, version, forceOptional);
      }
    } else if (stringSchema != null && stringSchema.getFormatValidator() != null) {
      if (numberSchema != null) {
        // This is a number or integer with a format
        return toConnectSchema(ctx, numberSchema, version, forceOptional);
      }
      return toConnectSchema(ctx, stringSchema, version, forceOptional);
    } else if (referenceSchema != null) {
      SchemaBuilder refBuilder = ctx.get(referenceSchema.getReferredSchema());
      if (refBuilder != null) {
        refBuilder.parameter(JSON_ID_PROP, DEFAULT_ID_PREFIX + ctx.incrementAndGetIdIndex());
        return new SchemaWrapper(refBuilder, forceOptional);
      } else {
        return toConnectSchema(ctx, referenceSchema.getReferredSchema(), version, forceOptional);
      }
    }
    throw new IllegalArgumentException("Unsupported criterion "
        + combinedSchema.getCriterion() + " for " + combinedSchema);
  }

  private void collectPropertySchemas(
      org.everit.json.schema.Schema schema,
      Map<String, org.everit.json.schema.Schema> properties,
      Map<String, Boolean> required,
      Set<JsonSchema> visited) {
    JsonSchema jsonSchema = new JsonSchema(schema);
    if (visited.contains(jsonSchema)) {
      return;
    } else {
      visited.add(jsonSchema);
    }
    if (schema instanceof CombinedSchema) {
      CombinedSchema combinedSchema = (CombinedSchema) schema;
      if (combinedSchema.getCriterion() == CombinedSchema.ALL_CRITERION) {
        for (org.everit.json.schema.Schema subSchema : combinedSchema.getSubschemas()) {
          collectPropertySchemas(subSchema, properties, required, visited);
        }
      }
    } else if (schema instanceof ObjectSchema) {
      ObjectSchema objectSchema = (ObjectSchema) schema;
      for (Map.Entry<String, org.everit.json.schema.Schema> entry
          : objectSchema.getPropertySchemas().entrySet()) {
        String fieldName = entry.getKey();
        properties.put(fieldName, entry.getValue());
        required.put(fieldName, objectSchema.getRequiredProperties().contains(fieldName));
      }
    } else if (schema instanceof ReferenceSchema) {
      ReferenceSchema refSchema = (ReferenceSchema) schema;
      collectPropertySchemas(refSchema.getReferredSchema(), properties, required, visited);
    }
  }

  private static boolean isUnionSchema(Schema schema) {
    return JSON_TYPE_ONE_OF.equals(schema.name()) || ConnectUnion.isUnion(schema);
  }

  private interface JsonToConnectTypeConverter {
    Object convert(JsonSchemaData data, Schema schema, JsonNode value);
  }

  private interface ConnectToJsonLogicalTypeConverter {
    JsonNode convert(Schema schema, Object value, JsonSchemaDataConfig config);
  }

  private interface JsonToConnectLogicalTypeConverter {
    Object convert(Schema schema, JsonNode value);
  }

  /**
   * Wraps a SchemaBuilder.
   * The internal builder should never be returned, so that the schema is not built prematurely.
   */
  static class SchemaWrapper extends SchemaBuilder {

    private final SchemaBuilder builder;
    // Optional that overrides the one in builder
    private boolean optional;
    // Parameters that override the ones in builder
    private final Map<String, String> parameters;

    public SchemaWrapper(SchemaBuilder builder, boolean optional) {
      super(Type.STRUCT);
      this.builder = builder;
      this.optional = optional;
      this.parameters = new LinkedHashMap<>();
    }

    @Override
    public boolean isOptional() {
      return optional;
    }

    @Override
    public SchemaBuilder optional() {
      optional = true;
      return this;
    }

    @Override
    public SchemaBuilder required() {
      optional = false;
      return this;
    }

    @Override
    public Object defaultValue() {
      return builder.defaultValue();
    }

    @Override
    public SchemaBuilder defaultValue(Object value) {
      builder.defaultValue(value);
      return this;
    }

    @Override
    public String name() {
      return builder.name();
    }

    @Override
    public SchemaBuilder name(String name) {
      builder.name(name);
      return this;
    }

    @Override
    public Integer version() {
      return builder.version();
    }

    @Override
    public SchemaBuilder version(Integer version) {
      builder.version(version);
      return this;
    }

    @Override
    public String doc() {
      return builder.doc();
    }

    @Override
    public SchemaBuilder doc(String doc) {
      builder.doc(doc);
      return this;
    }

    @Override
    public Map<String, String> parameters() {
      Map<String, String> allParameters = new HashMap<>();
      if (builder.parameters() != null) {
        allParameters.putAll(builder.parameters());
      }
      allParameters.putAll(parameters);
      return allParameters;
    }

    @Override
    public SchemaBuilder parameters(Map<String, String> props) {
      parameters.putAll(props);
      return this;
    }

    @Override
    public SchemaBuilder parameter(String propertyName, String propertyValue) {
      parameters.put(propertyName, propertyValue);
      return this;
    }

    @Override
    public Type type() {
      return builder.type();
    }

    @Override
    public List<Field> fields() {
      return builder.fields();
    }

    @Override
    public Field field(String fieldName) {
      return builder.field(fieldName);
    }

    @Override
    public SchemaBuilder field(String fieldName, Schema fieldSchema) {
      builder.field(fieldName, fieldSchema);
      return this;
    }

    @Override
    public Schema keySchema() {
      return builder.keySchema();
    }

    @Override
    public Schema valueSchema() {
      return builder.valueSchema();
    }

    @Override
    public Schema build() {
      // Don't create a ConnectSchema
      return this;
    }

    @Override
    public Schema schema() {
      // Don't create a ConnectSchema
      return this;
    }
  }

  /**
   * Class that holds the context for performing {@code toConnectSchema}
   */
  private static class ToConnectContext {
    private final Map<org.everit.json.schema.Schema, SchemaBuilder> schemaToStructMap;
    private int idIndex = 0;
    private int unionIndex = 0;

    public ToConnectContext() {
      this.schemaToStructMap = new IdentityHashMap<>();
    }

    public SchemaBuilder get(org.everit.json.schema.Schema schema) {
      return schemaToStructMap.get(schema);
    }

    public void put(org.everit.json.schema.Schema schema, SchemaBuilder builder) {
      schemaToStructMap.put(schema, builder);
    }

    public int incrementAndGetIdIndex() {
      return ++idIndex;
    }

    public int getAndIncrementUnionIndex() {
      return unionIndex++;
    }
  }

  /**
   * Class that holds the context for performing {@code fromConnectSchema}
   */
  private static class FromConnectContext {
    private final Set<String> ids;

    public FromConnectContext() {
      this.ids = new HashSet<>();
    }

    public boolean contains(String id) {
      return id != null && ids.contains(id);
    }

    public void add(String id) {
      if (id != null) {
        ids.add(id);
      }
    }
  }
}
