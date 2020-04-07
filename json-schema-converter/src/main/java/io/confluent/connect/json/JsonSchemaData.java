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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
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

  public static final String JSON_TYPE_ENUM = NAMESPACE + ".Enum";
  public static final String JSON_TYPE_ENUM_PREFIX = JSON_TYPE_ENUM + ".";
  public static final String JSON_TYPE_ONE_OF = NAMESPACE + ".OneOf";

  private static final JsonNodeFactory JSON_NODE_FACTORY =
      JsonNodeFactory.withExactBigDecimals(true);

  private static final Map<Schema.Type, JsonToConnectTypeConverter> TO_CONNECT_CONVERTERS =
      new EnumMap<>(
      Schema.Type.class);

  static {
    TO_CONNECT_CONVERTERS.put(Schema.Type.BOOLEAN, (schema, value) -> value.booleanValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.INT8, (schema, value) -> (byte) value.shortValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.INT16, (schema, value) -> value.shortValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.INT32, (schema, value) -> value.intValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.INT64, (schema, value) -> value.longValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT32, (schema, value) -> value.floatValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT64, (schema, value) -> value.doubleValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.BYTES, (schema, value) -> {
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
    TO_CONNECT_CONVERTERS.put(Schema.Type.STRING, (schema, value) -> value.textValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.ARRAY, (schema, value) -> {
      Schema elemSchema = schema == null ? null : schema.valueSchema();
      ArrayList<Object> result = new ArrayList<>();
      for (JsonNode elem : value) {
        result.add(toConnectData(elemSchema, elem));
      }
      return result;
    });
    TO_CONNECT_CONVERTERS.put(Schema.Type.MAP, (schema, value) -> {
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
          result.put(entry.getKey(), toConnectData(valueSchema, entry.getValue()));
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
          result.put(toConnectData(keySchema, entry.get(KEY_FIELD)),
              toConnectData(valueSchema, entry.get(VALUE_FIELD))
          );
        }
      }
      return result;
    });
    TO_CONNECT_CONVERTERS.put(Schema.Type.STRUCT, (schema, value) -> {
      if (schema.name() != null && schema.name().equals(JSON_TYPE_ONE_OF)) {
        int index = 0;
        for (Field field : schema.fields()) {
          Schema fieldSchema = field.schema();

          if (isInstanceOfSchemaTypeForSimpleSchema(fieldSchema, value)) {
            return new Struct(schema.schema()).put(JSON_TYPE_ONE_OF + ".field." + index,
                toConnectData(fieldSchema, value)
            );
          }
          index++;
        }
        throw new DataException("Did not find matching oneof field for data: " + value.toString());
      } else {
        if (!value.isObject()) {
          throw new DataException("Structs should be encoded as JSON objects, but found "
              + value.getNodeType());
        }

        Struct result = new Struct(schema.schema());
        for (Field field : schema.fields()) {
          result.put(field, toConnectData(field.schema(), value.get(field.name())));
        }

        return result;
      }
    });
  }

  private static boolean isInstanceOfSchemaTypeForSimpleSchema(Schema fieldSchema, JsonNode value) {
    switch (fieldSchema.type()) {
      case INT8:
      case INT16:
        return value.isShort();
      case INT32:
        return value.isInt();
      case INT64:
        return value.isLong();
      case FLOAT32:
        return value.isFloat();
      case FLOAT64:
        return value.isDouble();
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
        return value.isObject();
      default:
        throw new IllegalArgumentException("Unsupported type " + fieldSchema.type());
    }
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

  private JsonSchemaDataConfig config;
  private Cache<Schema, org.everit.json.schema.Schema> fromConnectSchemaCache;
  private Cache<JsonSchema, Schema> toConnectSchemaCache;

  public JsonSchemaData() {
    this(new JsonSchemaDataConfig.Builder().with(
        SCHEMAS_CACHE_SIZE_CONFIG,
        SCHEMAS_CACHE_SIZE_DEFAULT
    ).build());
  }

  public JsonSchemaData(JsonSchemaDataConfig jsonSchemaDataConfig) {
    this.config = jsonSchemaDataConfig;
    fromConnectSchemaCache =
        new SynchronizedCache<>(new LRUCache<>(jsonSchemaDataConfig.schemaCacheSize()));
    toConnectSchemaCache =
        new SynchronizedCache<>(new LRUCache<>(jsonSchemaDataConfig.schemaCacheSize()));
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
      if (schema.defaultValue() != null) {
        return fromConnectData(schema, schema.defaultValue());
      }
      if (schema.isOptional()) {
        return JSON_NODE_FACTORY.nullNode();
      }
      throw new DataException(
          "Conversion error: null value for field that is required and has no default value");
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
          if (JSON_TYPE_ONE_OF.equals(schema.name())) {
            for (Field field : schema.fields()) {
              Object object = struct.get(field);
              if (object != null) {
                return fromConnectData(field.schema(), object);
              }
            }
            return fromConnectData(schema, null);
          } else {
            ObjectNode obj = JSON_NODE_FACTORY.objectNode();
            for (Field field : schema.fields()) {
              obj.set(field.name(), fromConnectData(field.schema(), struct.get(field)));
            }
            return obj;
          }
        }
        default:
          break;
      }

      throw new DataException("Couldn't convert " + value + " to JSON.");
    } catch (ClassCastException e) {
      String schemaTypeStr = (schema != null) ? schema.type().toString() : "unknown schema";
      throw new DataException("Invalid type for " + schemaTypeStr + ": " + value.getClass());
    }
  }

  public static Object toConnectData(Schema schema, JsonNode jsonValue) {
    final Schema.Type schemaType;
    if (schema != null) {
      schemaType = schema.type();
      if (jsonValue == null || jsonValue.isNull()) {
        if (schema.defaultValue() != null) {
          return schema.defaultValue(); // any logical type conversions should already have been
        }
        // applied
        if (schema.isOptional()) {
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
    return typeConverter.convert(schema, jsonValue);
  }

  public JsonSchema fromConnectSchema(Schema schema) {
    return new JsonSchema(rawSchemaFromConnectSchema(schema));
  }

  private org.everit.json.schema.Schema rawSchemaFromConnectSchema(Schema schema) {
    if (schema == null) {
      return null;
    }
    org.everit.json.schema.Schema cachedSchema = fromConnectSchemaCache.get(schema);
    if (cachedSchema != null) {
      return cachedSchema;
    }
    org.everit.json.schema.Schema resultSchema = rawSchemaFromConnectSchema(schema, null);
    fromConnectSchemaCache.put(schema, resultSchema);
    return resultSchema;
  }

  private org.everit.json.schema.Schema rawSchemaFromConnectSchema(Schema schema, Integer index) {
    return rawSchemaFromConnectSchema(schema, index, false);
  }

  private org.everit.json.schema.Schema rawSchemaFromConnectSchema(
      Schema schema, Integer index, boolean ignoreOptional
  ) {
    if (schema == null) {
      return null;
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
        if (schema.parameters() != null && schema.parameters().containsKey(JSON_TYPE_ENUM)) {
          EnumSchema.Builder enumBuilder = EnumSchema.builder();
          for (Map.Entry<String, String> entry : schema.parameters().entrySet()) {
            if (entry.getKey().startsWith(JSON_TYPE_ENUM_PREFIX)) {
              enumBuilder.possibleValue(entry.getValue());
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
        builder = ArraySchema.builder().allItemSchema(
            rawSchemaFromConnectSchema(schema.valueSchema()));
        break;
      case MAP:
        // JSON Schema only supports string keys
        if (schema.keySchema().type() == Schema.Type.STRING && !schema.keySchema().isOptional()) {
          org.everit.json.schema.Schema valueSchema =
              rawSchemaFromConnectSchema(schema.valueSchema());
          builder = ObjectSchema.builder().schemaOfAdditionalProperties(valueSchema);
          unprocessedProps.put(CONNECT_TYPE_PROP, CONNECT_TYPE_MAP);
        } else {
          ObjectSchema.Builder entryBuilder = ObjectSchema.builder();
          org.everit.json.schema.Schema keySchema =
              rawSchemaFromConnectSchema(schema.keySchema(), 0);
          org.everit.json.schema.Schema valueSchema =
              rawSchemaFromConnectSchema(schema.valueSchema(), 1);
          entryBuilder.addPropertySchema(KEY_FIELD, keySchema);
          entryBuilder.addPropertySchema(VALUE_FIELD, valueSchema);
          builder = ArraySchema.builder().allItemSchema(entryBuilder.build());
          unprocessedProps.put(CONNECT_TYPE_PROP, CONNECT_TYPE_MAP);
        }
        break;
      case STRUCT:
        if (JSON_TYPE_ONE_OF.equals(schema.name())) {
          CombinedSchema.Builder combinedBuilder = CombinedSchema.builder();
          combinedBuilder.criterion(CombinedSchema.ONE_CRITERION);
          if (schema.isOptional()) {
            combinedBuilder.subschema(NullSchema.INSTANCE);
          }
          for (Field field : schema.fields()) {
            combinedBuilder.subschema(rawSchemaFromConnectSchema(nonOptional(field.schema()),
                field.index(),
                true
            ));
          }
          builder = combinedBuilder;
        } else if (schema.isOptional()) {
          CombinedSchema.Builder combinedBuilder = CombinedSchema.builder();
          combinedBuilder.criterion(CombinedSchema.ONE_CRITERION);
          combinedBuilder.subschema(NullSchema.INSTANCE);
          combinedBuilder.subschema(rawSchemaFromConnectSchema(nonOptional(schema)));
          builder = combinedBuilder;
        } else {
          ObjectSchema.Builder objectBuilder = ObjectSchema.builder();
          for (Field field : schema.fields()) {
            org.everit.json.schema.Schema fieldSchema = rawSchemaFromConnectSchema(field.schema(),
                field.index()
            );
            objectBuilder.addPropertySchema(field.name(), fieldSchema);
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
        builder.defaultValue(schema.defaultValue());
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
    if (index != null) {
      unprocessedProps.put(CONNECT_INDEX_PROP, index);
    }
    return builder.unprocessedProperties(unprocessedProps).build();
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
    Schema cachedSchema = toConnectSchemaCache.get(schema);
    if (cachedSchema != null) {
      return cachedSchema;
    }
    Schema resultSchema = toConnectSchema(schema.rawSchema(), schema.version(), false);
    toConnectSchemaCache.put(schema, resultSchema);
    return resultSchema;
  }

  @VisibleForTesting
  protected Schema toConnectSchema(org.everit.json.schema.Schema jsonSchema) {
    return toConnectSchema(jsonSchema, null);
  }

  private Schema toConnectSchema(org.everit.json.schema.Schema jsonSchema, Integer version) {
    return toConnectSchema(jsonSchema, version, false);
  }

  private Schema toConnectSchema(
      org.everit.json.schema.Schema jsonSchema, Integer version, boolean forceOptional
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
      builder.parameter(JSON_TYPE_ENUM,
          ""
      );  // JSON enums have no name, use empty string as placeholder
      for (Object enumObj : enumSchema.getPossibleValuesAsList()) {
        String enumSymbol = enumObj.toString();
        builder.parameter(JSON_TYPE_ENUM_PREFIX + enumSymbol, enumSymbol);
      }
    } else if (jsonSchema instanceof CombinedSchema) {
      CombinedSchema combinedSchema = (CombinedSchema) jsonSchema;
      CombinedSchema.ValidationCriterion criterion = combinedSchema.getCriterion();
      String name = null;
      if (criterion == CombinedSchema.ONE_CRITERION) {
        name = JSON_TYPE_ONE_OF;
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
          return toConnectSchema(nonNullSchema, version, true);
        }
      }
      int index = 0;
      builder = SchemaBuilder.struct().name(name);
      for (org.everit.json.schema.Schema subSchema : combinedSchema.getSubschemas()) {
        if (subSchema instanceof NullSchema) {
          builder.optional();
        } else {
          String subFieldName = name + ".field." + index++;
          builder.field(subFieldName, toConnectSchema(subSchema));
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
        builder = SchemaBuilder.map(toConnectSchema(objectSchema.getPropertySchemas()
                .get(KEY_FIELD)),
            toConnectSchema(objectSchema.getPropertySchemas().get(VALUE_FIELD))
        );
      } else {
        builder = SchemaBuilder.array(toConnectSchema(itemsSchema));
      }
    } else if (jsonSchema instanceof ObjectSchema) {
      ObjectSchema objectSchema = (ObjectSchema) jsonSchema;
      String type = (String) objectSchema.getUnprocessedProperties().get(CONNECT_TYPE_PROP);
      if (CONNECT_TYPE_MAP.equals(type)) {
        builder = SchemaBuilder.map(Schema.STRING_SCHEMA,
            toConnectSchema(objectSchema.getSchemaOfAdditionalProperties())
        );
      } else {
        builder = SchemaBuilder.struct();
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
          builder.field(subFieldName, toConnectSchema(subSchema));
        }
      }
    } else if (jsonSchema instanceof ReferenceSchema) {
      ReferenceSchema refSchema = (ReferenceSchema) jsonSchema;
      return toConnectSchema(refSchema.getReferredSchema(), version);
    } else {
      throw new DataException("Unsupported schema type " + jsonSchema.getClass().getName());
    }

    String title = jsonSchema.getTitle();
    if (title != null) {
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
      builder.defaultValue(jsonSchema.getDefaultValue());
    }

    if (forceOptional) {
      builder.optional();
    }

    Schema result = builder.build();
    return result;
  }

  private interface JsonToConnectTypeConverter {
    Object convert(Schema schema, JsonNode value);
  }

  private interface ConnectToJsonLogicalTypeConverter {
    JsonNode convert(Schema schema, Object value, JsonSchemaDataConfig config);
  }

  private interface JsonToConnectLogicalTypeConverter {
    Object convert(Schema schema, JsonNode value);
  }
}
