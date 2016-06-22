/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.connect.avro;

import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.NonRecordContainer;

import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utilities for converting between our runtime data format and Avro, and (de)serializing that data.
 */
public class AvroData {
  public static final String NAMESPACE = "io.confluent.connect.avro";
  // Avro does not permit empty schema names, which might be the ideal default since we also are
  // not permitted to simply omit the name. Instead, make it very clear where the default is
  // coming from.
  public static final String DEFAULT_SCHEMA_NAME = "ConnectDefault";
  public static final String DEFAULT_SCHEMA_FULL_NAME = NAMESPACE + "." + DEFAULT_SCHEMA_NAME;
  public static final String MAP_ENTRY_TYPE_NAME = "MapEntry";
  public static final String KEY_FIELD = "key";
  public static final String VALUE_FIELD = "value";

  public static final String CONNECT_NAME_PROP = "connect.name";
  public static final String CONNECT_DOC_PROP = "connect.doc";
  public static final String CONNECT_VERSION_PROP = "connect.version";
  public static final String CONNECT_DEFAULT_VALUE_PROP = "connect.default";
  public static final String CONNECT_PARAMETERS_PROP = "connect.parameters";

  public static final String CONNECT_TYPE_PROP = "connect.type";

  public static final String CONNECT_TYPE_INT8 = "int8";
  public static final String CONNECT_TYPE_INT16 = "int16";

  public static final String AVRO_TYPE_UNION = NAMESPACE + ".Union";
  public static final String AVRO_TYPE_ENUM = NAMESPACE + ".Enum";

  public static final String AVRO_TYPE_ANYTHING = NAMESPACE + ".Anything";

  private static final Map<String, Schema.Type> NON_AVRO_TYPES_BY_TYPE_CODE = new HashMap<>();
  static {
    NON_AVRO_TYPES_BY_TYPE_CODE.put(CONNECT_TYPE_INT8, Schema.Type.INT8);
    NON_AVRO_TYPES_BY_TYPE_CODE.put(CONNECT_TYPE_INT16, Schema.Type.INT16);
  }

  // Avro Java object types used by Connect schema types
  private static final Map<Schema.Type, List<Class>> SIMPLE_AVRO_SCHEMA_TYPES = new HashMap<>();
  static {
    SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.INT32, Arrays.asList((Class) Integer.class));
    SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.INT64, Arrays.asList((Class) Long.class));
    SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.FLOAT32, Arrays.asList((Class) Float.class));
    SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.FLOAT64, Arrays.asList((Class) Double.class));
    SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.BOOLEAN, Arrays.asList((Class) Boolean.class));
    SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.STRING, Arrays.asList((Class) CharSequence.class));
    SIMPLE_AVRO_SCHEMA_TYPES.put(
        Schema.Type.BYTES,
        Arrays.asList((Class) ByteBuffer.class, (Class) byte[].class, (Class) GenericFixed.class));
    SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.ARRAY, Arrays.asList((Class) Collection.class));
    SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.MAP, Arrays.asList((Class) Map.class));
  }

  private static final Map<Schema.Type, org.apache.avro.Schema.Type> CONNECT_TYPES_TO_AVRO_TYPES
      = new HashMap<>();
  static {
    CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.INT32, org.apache.avro.Schema.Type.INT);
    CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.INT64, org.apache.avro.Schema.Type.LONG);
    CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.FLOAT32, org.apache.avro.Schema.Type.FLOAT);
    CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.FLOAT64, org.apache.avro.Schema.Type.DOUBLE);
    CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.BOOLEAN, org.apache.avro.Schema.Type.BOOLEAN);
    CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.STRING, org.apache.avro.Schema.Type.STRING);
    CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.BYTES, org.apache.avro.Schema.Type.BYTES);
    CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.ARRAY, org.apache.avro.Schema.Type.ARRAY);
    CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.MAP, org.apache.avro.Schema.Type.MAP);
  }


  private static final String ANYTHING_SCHEMA_BOOLEAN_FIELD = "boolean";
  private static final String ANYTHING_SCHEMA_BYTES_FIELD = "bytes";
  private static final String ANYTHING_SCHEMA_DOUBLE_FIELD = "double";
  private static final String ANYTHING_SCHEMA_FLOAT_FIELD = "float";
  private static final String ANYTHING_SCHEMA_INT_FIELD = "int";
  private static final String ANYTHING_SCHEMA_LONG_FIELD = "long";
  private static final String ANYTHING_SCHEMA_STRING_FIELD = "string";
  private static final String ANYTHING_SCHEMA_ARRAY_FIELD = "array";
  private static final String ANYTHING_SCHEMA_MAP_FIELD = "map";

  public static final org.apache.avro.Schema ANYTHING_SCHEMA_MAP_ELEMENT;
  public static final org.apache.avro.Schema ANYTHING_SCHEMA;
  static {
    // Intuitively this should be a union schema. However, unions can't be named in Avro and this
    // is a self-referencing type, so we need to use a format in which we can name the entire schema

    ANYTHING_SCHEMA =
        org.apache.avro.SchemaBuilder.record(AVRO_TYPE_ANYTHING).namespace(NAMESPACE).fields()
            .optionalBoolean(ANYTHING_SCHEMA_BOOLEAN_FIELD)
            .optionalBytes(ANYTHING_SCHEMA_BYTES_FIELD)
            .optionalDouble(ANYTHING_SCHEMA_DOUBLE_FIELD)
            .optionalFloat(ANYTHING_SCHEMA_FLOAT_FIELD)
            .optionalInt(ANYTHING_SCHEMA_INT_FIELD)
            .optionalLong(ANYTHING_SCHEMA_LONG_FIELD)
            .optionalString(ANYTHING_SCHEMA_STRING_FIELD)
            .name(ANYTHING_SCHEMA_ARRAY_FIELD).type().optional().array()
              .items().type(AVRO_TYPE_ANYTHING)
            .name(ANYTHING_SCHEMA_MAP_FIELD).type().optional().array()
              .items().record(MAP_ENTRY_TYPE_NAME).namespace(NAMESPACE).fields()
            .name(KEY_FIELD).type(AVRO_TYPE_ANYTHING).noDefault()
            .name(VALUE_FIELD).type(AVRO_TYPE_ANYTHING).noDefault()
              .endRecord()
            .endRecord();
    // This is convenient to have extracted; we can't define it before ANYTHING_SCHEMA because it
    // uses ANYTHING_SCHEMA in its definition.
    ANYTHING_SCHEMA_MAP_ELEMENT = ANYTHING_SCHEMA.getField("map").schema()
        .getTypes().get(1) // The "map" field is optional, get the schema from the union type
        .getElementType();
  }


  // Convert values in Connect form into their logical types. These logical converters are discovered by logical type
  // names specified in the field
  private static final HashMap<String, LogicalTypeConverter> TO_CONNECT_LOGICAL_CONVERTERS = new HashMap<>();
  static {
    TO_CONNECT_LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, new LogicalTypeConverter() {
      @Override
      public Object convert(Schema schema, Object value) {
        if (value instanceof byte[]) {
          return Decimal.toLogical(schema, (byte[]) value);
        } else if (value instanceof ByteBuffer) {
          return Decimal.toLogical(schema, ((ByteBuffer) value).array());
        }
        throw new DataException("Invalid type for Decimal, underlying representation should be bytes but was " + value.getClass());
      }
    });

    TO_CONNECT_LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, new LogicalTypeConverter() {
      @Override
      public Object convert(Schema schema, Object value) {
        if (!(value instanceof Integer))
          throw new DataException("Invalid type for Date, underlying representation should be int32 but was " + value.getClass());
        return Date.toLogical(schema, (int) value);
      }
    });

    TO_CONNECT_LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, new LogicalTypeConverter() {
      @Override
      public Object convert(Schema schema, Object value) {
        if (!(value instanceof Integer))
          throw new DataException("Invalid type for Time, underlying representation should be int32 but was " + value.getClass());
        return Time.toLogical(schema, (int) value);
      }
    });

    TO_CONNECT_LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, new LogicalTypeConverter() {
      @Override
      public Object convert(Schema schema, Object value) {
        if (!(value instanceof Long))
          throw new DataException("Invalid type for Timestamp, underlying representation should be int64 but was " + value.getClass());
        return Timestamp.toLogical(schema, (long) value);
      }
    });
  }

  private static final HashMap<String, LogicalTypeConverter> TO_AVRO_LOGICAL_CONVERTERS
      = new HashMap<>();
  static {
    TO_AVRO_LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, new LogicalTypeConverter() {
      @Override
      public Object convert(Schema schema, Object value) {
        if (!(value instanceof BigDecimal))
          throw new DataException(
              "Invalid type for Decimal, expected BigDecimal but was " + value.getClass());
        return Decimal.fromLogical(schema, (BigDecimal) value);
      }
    });

    TO_AVRO_LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, new LogicalTypeConverter() {
      @Override
      public Object convert(Schema schema, Object value) {
        if (!(value instanceof java.util.Date))
          throw new DataException(
              "Invalid type for Date, expected Date but was " + value.getClass());
        return Date.fromLogical(schema, (java.util.Date) value);
      }
    });

    TO_AVRO_LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, new LogicalTypeConverter() {
      @Override
      public Object convert(Schema schema, Object value) {
        if (!(value instanceof java.util.Date))
          throw new DataException(
              "Invalid type for Time, expected Date but was " + value.getClass());
        return Time.fromLogical(schema, (java.util.Date) value);
      }
    });

    TO_AVRO_LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, new LogicalTypeConverter() {
      @Override
      public Object convert(Schema schema, Object value) {
        if (!(value instanceof java.util.Date))
          throw new DataException(
              "Invalid type for Timestamp, expected Date but was " + value.getClass());
        return Timestamp.fromLogical(schema, (java.util.Date) value);
      }
    });
  }

  private Cache<Schema, org.apache.avro.Schema> fromConnectSchemaCache;
  private Cache<org.apache.avro.Schema, Schema> toConnectSchemaCache;


  public AvroData(int cacheSize) {
    fromConnectSchemaCache = new LRUCache<>(cacheSize);
    toConnectSchemaCache = new LRUCache<>(cacheSize);
  }

  /**
   * Convert this object, in Connect data format, into an Avro object.
   */
  public Object fromConnectData(Schema schema, Object value) {
    org.apache.avro.Schema avroSchema = fromConnectSchema(schema);
    return fromConnectData(schema, avroSchema, value, true, false);
  }

  /**
   * Convert from Connect data format to Avro. This version assumes the Avro schema has already
   * been converted and makes the use of NonRecordContainer optional
   * @param schema the Connect schema
   * @param avroSchema the corresponding
   * @param logicalValue the Connect data to convert, which may be a value for a logical type
   * @param requireContainer if true, wrap primitives, maps, and arrays in a NonRecordContainer
   *                         before returning them
   * @param requireSchemalessContainerNull if true, use a container representation of null because
   *                                       this is part of struct/array/map and we cannot
   *                                       represent nulls as true null because Anything cannot
   *                                       be a union type; otherwise, this is a top-level value
   *                                       and can return null
   * @return the converted data
   */
  private static Object fromConnectData(Schema schema, org.apache.avro.Schema avroSchema,
                                       Object logicalValue, boolean requireContainer, boolean
                                            requireSchemalessContainerNull) {
    Schema.Type schemaType = schema != null ? schema.type() : schemaTypeForSchemalessJavaType(logicalValue);
    if (schemaType == null) {
      // Schemaless null data since schema is null and we got a null schema type from the value
      if (requireSchemalessContainerNull)
        return new GenericRecordBuilder(ANYTHING_SCHEMA).build();
      else
        return null;
    }

    validateSchemaValue(schema, logicalValue);

    if (logicalValue == null) {
      // But if this is schemaless, we may not be able to return null directly
      if (schema == null && requireSchemalessContainerNull)
        return new GenericRecordBuilder(ANYTHING_SCHEMA).build();
      else
        return null;
    }

    // If this is a logical type, convert it from the convenient Java type to the underlying
    // serializeable format
    Object value = logicalValue;
    if (schema != null && schema.name() != null) {
      LogicalTypeConverter logicalConverter = TO_AVRO_LOGICAL_CONVERTERS.get(schema.name());
      if (logicalConverter != null && logicalValue != null)
        value = logicalConverter.convert(schema, logicalValue);
    }

    try {
      switch (schemaType) {
        case INT8: {
          Byte byteValue = (Byte) value; // Check for correct type
          Integer convertedByteValue = byteValue == null ? null : byteValue.intValue();
          return maybeAddContainer(
              avroSchema,
              maybeWrapSchemaless(schema, convertedByteValue, ANYTHING_SCHEMA_INT_FIELD),
              requireContainer);
        }
        case INT16: {
          Short shortValue = (Short) value; // Check for correct type
          Integer convertedShortValue = shortValue == null ? null : shortValue.intValue();
          return maybeAddContainer(
              avroSchema,
              maybeWrapSchemaless(schema, convertedShortValue, ANYTHING_SCHEMA_INT_FIELD),
              requireContainer);
        }

        case INT32:
          Integer intValue = (Integer) value; // Check for correct type
          return maybeAddContainer(
              avroSchema,
              maybeWrapSchemaless(schema, value, ANYTHING_SCHEMA_INT_FIELD),
              requireContainer);
        case INT64:
          Long longValue = (Long) value; // Check for correct type
          return maybeAddContainer(
              avroSchema,
              maybeWrapSchemaless(schema, value, ANYTHING_SCHEMA_LONG_FIELD),
              requireContainer);
        case FLOAT32:
          Float floatValue = (Float) value; // Check for correct type
          return maybeAddContainer(
              avroSchema,
              maybeWrapSchemaless(schema, value, ANYTHING_SCHEMA_FLOAT_FIELD),
              requireContainer);
        case FLOAT64:
          Double doubleValue = (Double) value; // Check for correct type
          return maybeAddContainer(
              avroSchema,
              maybeWrapSchemaless(schema, value, ANYTHING_SCHEMA_DOUBLE_FIELD),
              requireContainer);
        case BOOLEAN:
          Boolean boolValue = (Boolean) value; // Check for correct type
          return maybeAddContainer(
              avroSchema,
              maybeWrapSchemaless(schema, value, ANYTHING_SCHEMA_BOOLEAN_FIELD),
              requireContainer);
        case STRING:
          String stringValue = (String) value; // Check for correct type
          return maybeAddContainer(
              avroSchema,
              maybeWrapSchemaless(schema, value, ANYTHING_SCHEMA_STRING_FIELD),
              requireContainer);

        case BYTES: {
          ByteBuffer bytesValue = value instanceof byte[] ? ByteBuffer.wrap((byte[]) value) :
                                  (ByteBuffer) value;
          return maybeAddContainer(
              avroSchema,
              maybeWrapSchemaless(schema, bytesValue, ANYTHING_SCHEMA_BYTES_FIELD),
              requireContainer);
        }

        case ARRAY: {
          Collection<Object> list = (Collection<Object>) value;
          // TODO most types don't need a new converted object since types pass through
          List<Object> converted = new ArrayList<>(list.size());
          Schema elementSchema = schema != null ? schema.valueSchema() : null;
          org.apache.avro.Schema underlyingAvroSchema = avroSchemaForUnderlyingTypeIfOptional(schema, avroSchema);
          org.apache.avro.Schema elementAvroSchema = 
              schema != null ? underlyingAvroSchema.getElementType() : ANYTHING_SCHEMA;
          for (Object val : list) {
            converted.add(
                fromConnectData(elementSchema, elementAvroSchema, val, false, true)
            );
          }
          return maybeAddContainer(
              avroSchema,
              maybeWrapSchemaless(schema, converted, ANYTHING_SCHEMA_ARRAY_FIELD),
              requireContainer);
        }

        case MAP: {
          Map<Object, Object> map = (Map<Object, Object>) value;
          org.apache.avro.Schema underlyingAvroSchema = avroSchemaForUnderlyingTypeIfOptional(schema, avroSchema);
          if (schema != null &&
              schema.keySchema().type() == Schema.Type.STRING && !schema.keySchema().isOptional()) {
            // TODO most types don't need a new converted object since types pass through
            Map<String, Object> converted = new HashMap<>();
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
              // Key is a String, no conversion needed
              Object convertedValue = fromConnectData(schema.valueSchema(),
                                                      underlyingAvroSchema.getValueType(),
                                                      entry.getValue(), false, true);
              converted.put((String)entry.getKey(), convertedValue);
            }
            return maybeAddContainer(avroSchema, converted, requireContainer);
          } else {
            List<GenericRecord> converted = new ArrayList<>(map.size());
            org.apache.avro.Schema elementSchema =
                schema != null ? underlyingAvroSchema.getElementType() : ANYTHING_SCHEMA_MAP_ELEMENT;
            org.apache.avro.Schema avroKeySchema = elementSchema.getField(KEY_FIELD).schema();
            org.apache.avro.Schema avroValueSchema = elementSchema.getField(VALUE_FIELD).schema();
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
              Object keyConverted = fromConnectData(schema != null ? schema.keySchema(): null,
                                                    avroKeySchema, entry.getKey(), false, true);
              Object valueConverted = fromConnectData(schema != null ? schema.valueSchema() : null,
                                                      avroValueSchema, entry.getValue(), false,
                                                      true);
              converted.add(
                      new GenericRecordBuilder(elementSchema)
                              .set(KEY_FIELD, keyConverted)
                              .set(VALUE_FIELD, valueConverted)
                              .build()
              );
            }
            return maybeAddContainer(
                avroSchema, maybeWrapSchemaless(schema, converted, ANYTHING_SCHEMA_MAP_FIELD),
                requireContainer);
          }
        }

        case STRUCT: {
          Struct struct = (Struct) value;
          if (!struct.schema().equals(schema))
            throw new DataException("Mismatching struct schema");
          org.apache.avro.Schema underlyingAvroSchema = avroSchemaForUnderlyingTypeIfOptional(schema, avroSchema);
          GenericRecordBuilder convertedBuilder = new GenericRecordBuilder(underlyingAvroSchema);
          for (Field field : schema.fields()) {
            org.apache.avro.Schema fieldAvroSchema = underlyingAvroSchema.getField(field.name()).schema();
            convertedBuilder.set(
                field.name(),
                fromConnectData(field.schema(), fieldAvroSchema, struct.get(field), false, true));
          }
          return convertedBuilder.build();
        }

        default:
          throw new DataException("Unknown schema type: " + schema.type());
      }
    } catch (ClassCastException e) {
      throw new DataException("Invalid type for " + schema.type() + ": " + value.getClass());
    }
  }
  
  /**
   * Connect optional fields are represented as a unions (null & type) in Avro
   * Return the Avro schema of the actual type in the Union (instead of the union itself)
   * @param schema
   * @param avroSchema
   * @return
   */
  private static org.apache.avro.Schema avroSchemaForUnderlyingTypeIfOptional(Schema schema, org.apache.avro.Schema avroSchema){
    
    if (schema != null && schema.isOptional()) {
      if (avroSchema.getType() == org.apache.avro.Schema.Type.UNION) {
        for (org.apache.avro.Schema typeSchema : avroSchema
            .getTypes()) {
          if (!typeSchema.getType().equals(
              org.apache.avro.Schema.Type.NULL)) {
              return typeSchema;
          }
        }
      } else {
        throw new DataException(
            "An optinal schema should have an Avro Union type, not "
                + schema.type());
      }
    }
    return avroSchema;
  }

  private static Schema.Type schemaTypeForSchemalessJavaType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Byte) {
      return Schema.Type.INT8;
    } else if (value instanceof Short) {
      return Schema.Type.INT16;
    } else if (value instanceof Integer) {
      return Schema.Type.INT32;
    } else if (value instanceof Long) {
      return Schema.Type.INT64;
    } else if (value instanceof Float) {
      return Schema.Type.FLOAT32;
    } else if (value instanceof Double) {
      return Schema.Type.FLOAT64;
    } else if (value instanceof Boolean) {
      return Schema.Type.BOOLEAN;
    } else if (value instanceof String) {
      return Schema.Type.STRING;
    } else if (value instanceof Collection) {
      return Schema.Type.ARRAY;
    } else if (value instanceof Map) {
      return Schema.Type.MAP;
    } else {
      throw new DataException("Unknown Java type for schemaless data: " + value.getClass());
    }
  }

  private static Object maybeAddContainer(org.apache.avro.Schema avroSchema, Object value,
                                          boolean wrap) {
    return wrap ? new NonRecordContainer(avroSchema, value) : value;
  }

  private static Object maybeWrapSchemaless(Schema schema, Object value, String typeField) {
    if (schema != null) {
      return value;
    }

    GenericRecordBuilder builder = new GenericRecordBuilder(ANYTHING_SCHEMA);
    if (value != null) {
      builder.set(typeField, value);
    }
    return builder.build();
  }

  public org.apache.avro.Schema fromConnectSchema(Schema schema) {
    if (schema == null) {
      return ANYTHING_SCHEMA;
    }

    org.apache.avro.Schema cached = fromConnectSchemaCache.get(schema);
    if (cached != null) {
      return cached;
    }

    String namespace = NAMESPACE;
    String name = DEFAULT_SCHEMA_NAME;
    if (schema.name() != null) {
      String[] split = splitName(schema.name());
      namespace = split[0];
      name = split[1];
    }

    // Extra type annotation information for otherwise lossy conversions
    String connectType = null;

    final org.apache.avro.Schema baseSchema;
    switch (schema.type()) {
      case INT8:
        connectType = CONNECT_TYPE_INT8;
        baseSchema = org.apache.avro.SchemaBuilder.builder().intType();
        break;
      case INT16:
        connectType = CONNECT_TYPE_INT16;
        baseSchema = org.apache.avro.SchemaBuilder.builder().intType();
        break;
      case INT32:
        baseSchema = org.apache.avro.SchemaBuilder.builder().intType();
        break;
      case INT64:
        baseSchema = org.apache.avro.SchemaBuilder.builder().longType();
        break;
      case FLOAT32:
        baseSchema = org.apache.avro.SchemaBuilder.builder().floatType();
        break;
      case FLOAT64:
        baseSchema = org.apache.avro.SchemaBuilder.builder().doubleType();
        break;
      case BOOLEAN:
        baseSchema = org.apache.avro.SchemaBuilder.builder().booleanType();
        break;
      case STRING:
        baseSchema = org.apache.avro.SchemaBuilder.builder().stringType();
        break;
      case BYTES:
        baseSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
        break;
      case ARRAY:
        baseSchema = org.apache.avro.SchemaBuilder.builder().array()
            .items(fromConnectSchema(schema.valueSchema()));
        break;
      case MAP:
        // Avro only supports string keys, so we match the representation when possible, but
        // otherwise fall back on a record representation
        if (schema.keySchema().type() == Schema.Type.STRING && !schema.keySchema().isOptional()) {
          baseSchema = org.apache.avro.SchemaBuilder.builder()
                  .map().values(fromConnectSchema(schema.valueSchema()));
        } else {
          // Special record name indicates format
          org.apache.avro.SchemaBuilder.FieldAssembler<org.apache.avro.Schema> fieldAssembler
              = org.apache.avro.SchemaBuilder.builder()
              .array().items()
              .record(MAP_ENTRY_TYPE_NAME).namespace(NAMESPACE).fields();
          addAvroRecordField(fieldAssembler, KEY_FIELD, schema.keySchema());
          addAvroRecordField(fieldAssembler, VALUE_FIELD, schema.valueSchema());
          baseSchema = fieldAssembler.endRecord();
        }
        break;
      case STRUCT:
        org.apache.avro.SchemaBuilder.FieldAssembler<org.apache.avro.Schema> fieldAssembler
            = org.apache.avro.SchemaBuilder
            .record(name != null ? name : DEFAULT_SCHEMA_NAME).namespace(namespace).fields();
        for (Field field : schema.fields()) {
          addAvroRecordField(fieldAssembler, field.name(), field.schema());
        }
        baseSchema = fieldAssembler.endRecord();
        break;
      default:
        throw new DataException("Unknown schema type: " + schema.type());
    }

    if (schema.doc() != null) {
      baseSchema.addProp(CONNECT_DOC_PROP, schema.doc());
    }
    if (schema.version() != null) {
      baseSchema.addProp(CONNECT_VERSION_PROP,
                         JsonNodeFactory.instance.numberNode(schema.version()));
    }
    if (schema.parameters() != null) {
      baseSchema.addProp(CONNECT_PARAMETERS_PROP, parametersFromConnect(schema.parameters()));
    }
    if (schema.defaultValue() != null) {
      baseSchema.addProp(CONNECT_DEFAULT_VALUE_PROP,
                         defaultValueFromConnect(schema, schema.defaultValue()));
    }

    // Only Avro named types (record, enum, fixed) may contain namespace + name. Only Connect's
    // struct converts to one of those (record), so for everything else that has a name we store
    // the full name into a special property. For uniformity, we also duplicate this info into
    // the same field in records as well even though it will also be available in the namespace()
    // and name().
    if (schema.name() != null) {
      baseSchema.addProp(CONNECT_NAME_PROP, schema.name());
    }

    // Some Connect types need special annotations to preserve the types accurate due to
    // limitations in Avro. These types get an extra annotation with their Connect type
    if (connectType != null) {
      baseSchema.addProp(CONNECT_TYPE_PROP, connectType);
    }

    // Note that all metadata has already been processed and placed on the baseSchema because we
    // can't store any metadata on the actual top-level schema when it's a union because of Avro
    // constraints on the format of schemas.
    org.apache.avro.Schema finalSchema = baseSchema;
    if (schema.isOptional()) {
      if (schema.defaultValue() != null)
        finalSchema = org.apache.avro.SchemaBuilder.builder().unionOf()
            .type(baseSchema).and()
            .nullType()
            .endUnion();
      else
        finalSchema = org.apache.avro.SchemaBuilder.builder().unionOf()
            .nullType().and()
            .type(baseSchema)
            .endUnion();
    }
    fromConnectSchemaCache.put(schema, finalSchema);
    return finalSchema;
  }


  private void addAvroRecordField(
      org.apache.avro.SchemaBuilder.FieldAssembler<org.apache.avro.Schema> fieldAssembler,
      String fieldName, Schema fieldSchema)
  {
    org.apache.avro.SchemaBuilder.GenericDefault<org.apache.avro.Schema> fieldAvroSchema
        = fieldAssembler.name(fieldName).type(fromConnectSchema(fieldSchema));
    if (fieldSchema.defaultValue() != null) {
      fieldAvroSchema.withDefault(defaultValueFromConnect(fieldSchema, fieldSchema.defaultValue()));
    } else {
      fieldAvroSchema.noDefault();
    }
  }

  // Convert default values from Connect data format to Avro's format, which is an
  // org.codehaus.jackson.JsonNode. The default value is provided as an argument because even
  // though you can get a default value from the schema, default values for complex structures need
  // to perform the same translation but those defaults will be part of the original top-level
  // (complex type) default value, not part of the child schema.
  private static JsonNode defaultValueFromConnect(Schema schema, Object defaultVal) {
    try {
      switch (schema.type()) {
        case INT8:
          return JsonNodeFactory.instance.numberNode((Byte) defaultVal);
        case INT16:
          return JsonNodeFactory.instance.numberNode((Short) defaultVal);
        case INT32:
          return JsonNodeFactory.instance.numberNode((Integer) defaultVal);
        case INT64:
          return JsonNodeFactory.instance.numberNode((Long) defaultVal);
        case FLOAT32:
          return JsonNodeFactory.instance.numberNode((Float) defaultVal);
        case FLOAT64:
          return JsonNodeFactory.instance.numberNode((Double) defaultVal);
        case BOOLEAN:
          return JsonNodeFactory.instance.booleanNode((Boolean) defaultVal);
        case STRING:
          return JsonNodeFactory.instance.textNode((String) defaultVal);
        case BYTES:
          if (defaultVal instanceof byte[])
            return JsonNodeFactory.instance.binaryNode((byte[]) defaultVal);
          else
            return JsonNodeFactory.instance.binaryNode(((ByteBuffer) defaultVal).array());
        case ARRAY: {
          ArrayNode array = JsonNodeFactory.instance.arrayNode();
          for (Object elem : (List<Object>) defaultVal) {
            array.add(defaultValueFromConnect(schema.valueSchema(), elem));
          }
          return array;
        }
        case MAP:
          if (schema.keySchema().type() == Schema.Type.STRING && !schema.keySchema().isOptional()) {
            ObjectNode node = JsonNodeFactory.instance.objectNode();
            for(Map.Entry<String, Object> entry : ((Map<String, Object>) defaultVal).entrySet()) {
              JsonNode entryDef = defaultValueFromConnect(schema.valueSchema(), entry.getValue());
              node.put(entry.getKey(), entryDef);
            }
            return node;
          } else {
            ArrayNode array = JsonNodeFactory.instance.arrayNode();
            for(Map.Entry<Object, Object> entry : ((Map<Object, Object>) defaultVal).entrySet()) {
              JsonNode keyDefault = defaultValueFromConnect(schema.keySchema(), entry.getKey());
              JsonNode valDefault = defaultValueFromConnect(schema.valueSchema(), entry.getValue());
              ArrayNode jsonEntry = JsonNodeFactory.instance.arrayNode();
              jsonEntry.add(keyDefault);
              jsonEntry.add(valDefault);
              array.add(jsonEntry);
            }
            return array;
          }
        case STRUCT: {
          ObjectNode node = JsonNodeFactory.instance.objectNode();
          Struct struct = ((Struct) defaultVal);
          for(Field field : (schema.fields())) {
            JsonNode fieldDef = defaultValueFromConnect(field.schema(), struct.get(field));
            node.put(field.name(), fieldDef);
          }
          return node;
        }
        default:
          throw new DataException("Unknown schema type:" + schema.type());
      }
    } catch (ClassCastException e) {
      throw new DataException("Invalid type used for default value of " + schema.type() +
                              " field: " + schema.defaultValue().getClass());
    }
  }

  private static JsonNode parametersFromConnect(Map<String, String> params) {
    ObjectNode result = JsonNodeFactory.instance.objectNode();
    for (Map.Entry<String, String> entry : params.entrySet()) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }

  private static void validateSchemaValue(Schema schema, Object value) throws DataException{
      if (value == null && schema != null && !schema.isOptional()) {
        throw new DataException("Found null value for non-optional schema");
      }
  }
  
  /**
   * Convert the given object, in Avro format, into an Connect data object.
   */
  public SchemaAndValue toConnectData(org.apache.avro.Schema avroSchema, Object value) {
    if (value == null) {
      return null;
    }

    Schema schema = (avroSchema.equals(ANYTHING_SCHEMA)) ? null : toConnectSchema(avroSchema);
    return new SchemaAndValue(schema, toConnectData(schema, value));
  }

  private Object toConnectData(Schema schema, Object value) {
    validateSchemaValue(schema, value);
    if (value == null) {
      return null;
    }  
    try {
      // If we're decoding schemaless data, we need to unwrap it into just the single value
      if (schema == null) {
        if (!(value instanceof IndexedRecord)) {
          throw new DataException("Invalid Avro data for schemaless Connect data");
        }
        IndexedRecord recordValue = (IndexedRecord) value;

        Object
            boolVal =
            recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_BOOLEAN_FIELD).pos());
        if (boolVal != null)
          return toConnectData(Schema.BOOLEAN_SCHEMA, boolVal);

        Object
            bytesVal =
            recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_BYTES_FIELD).pos());
        if (bytesVal != null)
          return toConnectData(Schema.BYTES_SCHEMA, bytesVal);

        Object
            dblVal =
            recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_DOUBLE_FIELD).pos());
        if (dblVal != null)
          return toConnectData(Schema.FLOAT64_SCHEMA, dblVal);

        Object
            fltVal =
            recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_FLOAT_FIELD).pos());
        if (fltVal != null)
          return toConnectData(Schema.FLOAT32_SCHEMA, fltVal);

        Object intVal = recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_INT_FIELD).pos());
        if (intVal != null)
          return toConnectData(Schema.INT32_SCHEMA, intVal);

        Object
            longVal =
            recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_LONG_FIELD).pos());
        if (longVal != null)
          return toConnectData(Schema.INT64_SCHEMA, longVal);

        Object
            stringVal =
            recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_STRING_FIELD).pos());
        if (stringVal != null)
          return toConnectData(Schema.STRING_SCHEMA, stringVal);

        Object
            arrayVal =
            recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_ARRAY_FIELD).pos());
        if (arrayVal != null)
          return toConnectData(SchemaBuilder.array(null).build(), arrayVal);

        Object mapVal = recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_MAP_FIELD).pos());
        if (mapVal != null)
          return toConnectData(SchemaBuilder.map(null, null).build(), mapVal);

        // If nothing was set, it's null
        return null;
      }

      Object converted = null;
      switch (schema.type()) {
        // Pass through types
        case INT32: {
          Integer intValue = (Integer) value; // Validate type
          converted = value;
          break;
        }
        case INT64: {
          Long longValue = (Long) value; // Validate type
          converted = value;
          break;
        }
        case FLOAT32: {
          Float floatValue = (Float) value; // Validate type
          converted = value;
          break;
        }
        case FLOAT64: {
          Double doubleValue = (Double) value; // Validate type
          converted = value;
          break;
        }
        case BOOLEAN: {
          Boolean boolValue = (Boolean) value; // Validate type
          converted = value;
          break;
        }

        case INT8:
          // Encoded as an Integer
          converted = value == null ? null : ((Integer) value).byteValue();
          break;
        case INT16:
          // Encoded as an Integer
          converted = value == null ? null : ((Integer) value).shortValue();
          break;

        case STRING:
          if (value instanceof String) {
            converted = value;
          } else if (value instanceof CharSequence ||
                     value instanceof GenericEnumSymbol ||
                     value instanceof Enum) {
            converted = value.toString();
          } else {
            throw new DataException("Invalid class for string type, expecting String or "
                                    + "CharSequence but found " + value.getClass());
          }
          break;

        case BYTES:
          if (value instanceof byte[]) {
            converted = ByteBuffer.wrap((byte[]) value);
          } else if (value instanceof ByteBuffer) {
            converted = value;
          } else if (value instanceof GenericFixed) {
            converted = ByteBuffer.wrap(((GenericFixed) value).bytes());
          } else {
            throw new DataException("Invalid class for bytes type, expecting byte[] or ByteBuffer "
                                    + "but found " + value.getClass());
          }
          break;

        case ARRAY: {
          Schema valueSchema = schema.valueSchema();
          Collection<Object> original = (Collection<Object>) value;
          List<Object> result = new ArrayList<>(original.size());
          for (Object elem : original) {
            result.add(toConnectData(valueSchema, elem));
          }
          converted = result;
          break;
        }

        case MAP: {
          Schema keySchema = schema.keySchema();
          Schema valueSchema = schema.valueSchema();
          if (keySchema != null && keySchema.type() == Schema.Type.STRING && !keySchema
              .isOptional()) {
            // String keys
            Map<CharSequence, Object> original = (Map<CharSequence, Object>) value;
            Map<CharSequence, Object> result = new HashMap<>(original.size());
            for (Map.Entry<CharSequence, Object> entry : original.entrySet()) {
              result.put(entry.getKey().toString(),
                         toConnectData(valueSchema, entry.getValue()));
            }
            converted = result;
          } else {
            // Arbitrary keys
            List<IndexedRecord> original = (List<IndexedRecord>) value;
            Map<Object, Object> result = new HashMap<>(original.size());
            for (IndexedRecord entry : original) {
              int avroKeyFieldIndex = entry.getSchema().getField(KEY_FIELD).pos();
              int avroValueFieldIndex = entry.getSchema().getField(VALUE_FIELD).pos();
              Object convertedKey = toConnectData(keySchema, entry.get(avroKeyFieldIndex));
              Object convertedValue = toConnectData(valueSchema, entry.get(avroValueFieldIndex));
              result.put(convertedKey, convertedValue);
            }
            converted = result;
          }
          break;
        }

        case STRUCT: {
          // Special case support for union types
          if (schema.name() != null && schema.name().equals(AVRO_TYPE_UNION)) {
            Schema valueRecordSchema = null;
            if (value instanceof IndexedRecord) {
              IndexedRecord valueRecord = ((IndexedRecord) value);
              valueRecordSchema = toConnectSchema(valueRecord.getSchema(), true, null, null);
            }
            for (Field field : schema.fields()) {
              Schema fieldSchema = field.schema();

              if (isInstanceOfAvroSchemaTypeForSimpleSchema(fieldSchema, value) ||
                  (valueRecordSchema != null && valueRecordSchema.equals(fieldSchema))) {
                converted = new Struct(schema).put(unionMemberFieldName(fieldSchema),
                                                   toConnectData(fieldSchema, value));
                break;
              }
            }
            if (converted == null) {
              throw new DataException(
                  "Did not find matching union field for data: " + value.toString());
            }
          } else {
            IndexedRecord original = (IndexedRecord) value;
            Struct result = new Struct(schema);
            for (Field field : schema.fields()) {
              int avroFieldIndex = original.getSchema().getField(field.name()).pos();
              Object convertedFieldValue
                  = toConnectData(field.schema(), original.get(avroFieldIndex));
              result.put(field, convertedFieldValue);
            }
            converted = result;
          }
          break;
        }

        default:
          throw new DataException("Unknown Connect schema type: " + schema.type());
      }

      if (schema != null && schema.name() != null) {
        LogicalTypeConverter logicalConverter = TO_CONNECT_LOGICAL_CONVERTERS.get(schema.name());
        if (logicalConverter != null)
          converted = logicalConverter.convert(schema, converted);
      }
      return converted;
    } catch (ClassCastException e) {
      throw new DataException("Invalid type for " + schema.type() + ": " + value.getClass());
    }
  }

  public Schema toConnectSchema(org.apache.avro.Schema schema) {
    // We perform caching only at this top level. While it might be helpful to cache some more of
    // the internal conversions, this is the safest place to add caching since some of the internal
    // conversions take extra flags (like forceOptional) which means the resulting schema might not
    // exactly match the Avro schema.
    Schema cachedSchema = toConnectSchemaCache.get(schema);
    if (cachedSchema != null) {
      return cachedSchema;
    }

    Schema resultSchema = toConnectSchema(schema, false, null, null);
    toConnectSchemaCache.put(schema, resultSchema);
    return resultSchema;
  }

  /**
   *
   * @param schema schema to convert
   * @param forceOptional make the resulting schema optional, for converting Avro unions to a
   *                      record format and simple Avro unions of null + type to optional schemas
   * @param fieldDefaultVal if non-null, override any connect-annotated default values with this
   *                        one; used when converting Avro record fields since they define
   *                        default values with the field spec, but Connect specifies them with
   *                        the field's schema
   * @param docDefaultVal if non-null, override any connect-annotated documentation with this
   *                      one; used when converting Avro record fields since they define doc
   *                      values with the field spec, but Connect specifies them with the field's
   *                      schema
   * @return
   */
  private Schema toConnectSchema(org.apache.avro.Schema schema, boolean forceOptional,
                                Object fieldDefaultVal, String docDefaultVal) {
    final SchemaBuilder builder;
    switch (schema.getType()) {
      case BOOLEAN:
        builder = SchemaBuilder.bool();
        break;
      case BYTES:
      case FIXED:
        builder = SchemaBuilder.bytes();
        break;

      case DOUBLE:
        builder = SchemaBuilder.float64();
        break;
      case FLOAT:
        builder = SchemaBuilder.float32();
        break;
      case INT:
        // INT is used for Connect's INT8, INT16, and INT32
        String type = schema.getProp(CONNECT_TYPE_PROP);
        if (type == null) {
          builder = SchemaBuilder.int32();
        } else {
          Schema.Type connectType = NON_AVRO_TYPES_BY_TYPE_CODE.get(type);
          if (connectType == null) {
            throw new DataException("Invalid Connect type annotation for Avro int field: " +
                                    connectType);
          }
          builder = SchemaBuilder.type(connectType);
        }
        break;
      case LONG:
        builder = SchemaBuilder.int64();
        break;
      case STRING:
        builder = SchemaBuilder.string();
        break;


      case ARRAY:
        org.apache.avro.Schema elemSchema = schema.getElementType();
        // Special case for custom encoding of non-string maps as list of key-value records
        if (elemSchema.getType().equals(org.apache.avro.Schema.Type.RECORD) &&
            elemSchema.getNamespace().equals(NAMESPACE) &&
            elemSchema.getName().equals(MAP_ENTRY_TYPE_NAME)) {
          if (elemSchema.getFields().size() != 2 ||
              elemSchema.getField(KEY_FIELD) == null ||
              elemSchema.getField(VALUE_FIELD) == null) {
            throw new DataException("Found map encoded as array of key-value pairs, but array "
                                    + "elements do not match the expected format.");
          }
          builder = SchemaBuilder.map(
              toConnectSchema(elemSchema.getField(KEY_FIELD).schema()),
              toConnectSchema(elemSchema.getField(VALUE_FIELD).schema())
          );
        } else {
          builder = SchemaBuilder.array(toConnectSchema(schema.getElementType()));
        }
        break;

      case MAP:
        builder = SchemaBuilder.map(Schema.STRING_SCHEMA, toConnectSchema(schema.getValueType()));
        break;

      case RECORD: {
        builder = SchemaBuilder.struct();
        for(org.apache.avro.Schema.Field field : schema.getFields()) {
          Schema fieldSchema = toConnectSchema(field.schema(), false, field.defaultValue(), field.doc());
          builder.field(field.name(), fieldSchema);
        }
        break;
      }

      case ENUM:
        // enums are unwrapped to strings and the original enum is not preserved
        builder = SchemaBuilder.string();
        break;

      case UNION: {
        // Connect doesn't support unions. To handle this, we convert them to records with field
        // names associated with each of the input types
        //
        // However, we also need to first check for union types that are used to indicate optional
        // fields. Since we don't support the null type, we'll consider any union containing a
        // null type as an indication that it should be handled as an optional field
        boolean hasNullSchema = false;
        org.apache.avro.Schema optionalSchema = null;
        for (org.apache.avro.Schema memberSchema : schema.getTypes()) {
          if (memberSchema.getType() == org.apache.avro.Schema.Type.NULL) {
            hasNullSchema = true;
          } else {
            optionalSchema = memberSchema;
          }
        }
        if (hasNullSchema) {
          // Optional fields may only have one other type
          if (schema.getTypes().size() != 2) {
            throw new DataException("Avro union types containing null are only supported as "
                                    + "optional fields and should have exactly two entries.");
          }
          // No builder needed here -- any metadata or defaults can be found on the member schema
          return toConnectSchema(optionalSchema, true, null, null);
        } else {
          builder = SchemaBuilder.struct().name(AVRO_TYPE_UNION);
          Set<String> fieldNames = new HashSet<>();
          for (org.apache.avro.Schema memberSchema : schema.getTypes()) {
            String fieldName = unionMemberFieldName(memberSchema);
            if (fieldNames.contains(fieldName)) {
              throw new DataException("Multiple union schemas map to the Connect union field name");
            }
            fieldNames.add(fieldName);
            builder.field(fieldName, toConnectSchema(memberSchema, true, null, null));
          }
        }
        break;
      }

      case NULL:
        // There's no dedicated null type in Connect. However, it also doesn't make sense to have a
        // standalone null type -- it wouldn't provide any useful information. Instead, it should
        // only be used in union types.
        throw new DataException("Standalone null schemas are not supported by this converter");

      default:
        throw new DataException("Couldn't translate unsupported schema type "
                                + schema.getType().getName() + ".");
    }


    String docVal = docDefaultVal != null ? docDefaultVal :
                    (schema.getDoc() != null ? schema.getDoc() : schema.getProp(CONNECT_DOC_PROP));
    if (docVal != null) {
      builder.doc(docVal);
    }

    // Included Kafka Connect version takes priority, fall back to schema registry version
    JsonNode version = schema.getJsonProp(CONNECT_VERSION_PROP);
    if (version == null) {
      version = schema.getJsonProp(
          AbstractKafkaAvroDeserializer.SCHEMA_REGISTRY_SCHEMA_VERSION_PROP);
    }
    if (version != null) {
      if (!version.isIntegralNumber()) {
        throw new DataException("Invalid Connect version found: " + version.toString());
      }
      builder.version(version.asInt());
    }

    JsonNode parameters = schema.getJsonProp(CONNECT_PARAMETERS_PROP);
    if (parameters != null) {
      if (!parameters.isObject()) {
        throw new DataException("Expected JSON object for schema parameters but found: " +
                                parameters);
      }
      Iterator<Map.Entry<String, JsonNode>> paramIt = parameters.getFields();
      while (paramIt.hasNext()) {
        Map.Entry<String, JsonNode> field = paramIt.next();
        JsonNode jsonValue = field.getValue();
        if (!jsonValue.isTextual()) {
          throw new DataException("Expected schema parameter values to be strings but found: " +
                                  jsonValue);
        }
        builder.parameter(field.getKey(), jsonValue.getTextValue());
      }
    }

    if (fieldDefaultVal == null) {
      fieldDefaultVal = schema.getJsonProp(CONNECT_DEFAULT_VALUE_PROP);
    }
    if (fieldDefaultVal != null) {
      builder.defaultValue(defaultValueFromAvro(builder, schema, fieldDefaultVal));
    }

    JsonNode connectNameJson = schema.getJsonProp(CONNECT_NAME_PROP);
    String name = null;
    if (connectNameJson != null) {
      if (!connectNameJson.isTextual()) {
        throw new DataException("Invalid schema name: " + connectNameJson.toString());
      }
      name = connectNameJson.asText();

    } else if (schema.getType() == org.apache.avro.Schema.Type.RECORD ||
               schema.getType() == org.apache.avro.Schema.Type.ENUM) {
      name = schema.getFullName();
    }
    if (name != null && !name.equals(DEFAULT_SCHEMA_FULL_NAME)) {
      builder.name(name);
    }

    if (forceOptional) {
      builder.optional();
    }

    return builder.build();
  }


  private Object defaultValueFromAvro(Schema schema,
                                      org.apache.avro.Schema avroSchema,
                                      Object value) {
    // The type will be JsonNode if this default was pulled from a Connect default field, or an
    // Object if it's the actual Avro-specified default. If it's a regular Java object, we can
    // use our existing conversion tools.
    if (!(value instanceof JsonNode)) {
      return toConnectData(schema, value);
    }

    JsonNode jsonValue = (JsonNode) value;
    switch (avroSchema.getType()) {
      case INT:
        if (schema.type() == Schema.Type.INT8)
          return (byte) jsonValue.getIntValue();
        else if (schema.type() == Schema.Type.INT16)
          return (short) jsonValue.getIntValue();
        else if (schema.type() == Schema.Type.INT32)
          return jsonValue.getIntValue();

      case LONG:
        return jsonValue.getLongValue();

      case FLOAT:
        return (float)jsonValue.getDoubleValue();
      case DOUBLE:
        return jsonValue.getDoubleValue();

      case BOOLEAN:
        return jsonValue.asBoolean();

      case NULL:
        return null;

      case STRING:
      case ENUM:
        return jsonValue.asText();

      case BYTES:
      case FIXED:
        try {
          return jsonValue.getBinaryValue();
        } catch (IOException e) {
          throw new DataException("Invalid binary data in default value");
        }

      case ARRAY: {
        if (!jsonValue.isArray()) {
          throw new DataException("Invalid JSON for array default value: " + jsonValue.toString());
        }
        List<Object> result = new ArrayList<>(jsonValue.size());
        for (JsonNode elem : jsonValue) {
          result.add(defaultValueFromAvro(schema, avroSchema.getElementType(), elem));
        }
        return result;
      }

      case MAP: {
        if (!jsonValue.isObject()) {
          throw new DataException("Invalid JSON for map default value: " + jsonValue.toString());
        }
        Map<String, Object> result = new HashMap<>(jsonValue.size());
        Iterator<Map.Entry<String, JsonNode>> fieldIt = jsonValue.getFields();
        while (fieldIt.hasNext()) {
          Map.Entry<String, JsonNode> field = fieldIt.next();
          Object converted = defaultValueFromAvro(schema, avroSchema.getElementType(),
                                                  field.getValue());
          result.put(field.getKey(), converted);
        }
        return result;
      }

      case RECORD:
      {
        if (!jsonValue.isObject()) {
          throw new DataException("Invalid JSON for record default value: " + jsonValue.toString());
        }

        Struct result = new Struct(schema);
        for (org.apache.avro.Schema.Field avroField : avroSchema.getFields()) {
          Field field = schema.field(avroField.name());
          JsonNode fieldJson = ((JsonNode) value).get(field.name());
          Object converted = defaultValueFromAvro(field.schema(), avroField.schema(), fieldJson);
          result.put(avroField.name(), converted);
        }
        return result;
      }

      case UNION: {
        // Defaults must match first type
        org.apache.avro.Schema memberAvroSchema = avroSchema.getTypes().get(0);
        return defaultValueFromAvro(schema.field(unionMemberFieldName(memberAvroSchema)).schema(),
                                    memberAvroSchema, value);
      }
    }

    return null;
  }


  private static String unionMemberFieldName(org.apache.avro.Schema schema) {
    if (schema.getType() == org.apache.avro.Schema.Type.RECORD ||
        schema.getType() == org.apache.avro.Schema.Type.ENUM) {
      return splitName(schema.getName())[1];
    }
    return schema.getType().getName();
  }

  private static String unionMemberFieldName(Schema schema) {
    if (schema.type() == Schema.Type.STRUCT || isEnumSchema(schema)) {
      return splitName(schema.name())[1];
    }
    return CONNECT_TYPES_TO_AVRO_TYPES.get(schema.type()).getName();
  }

  private static boolean isEnumSchema(Schema schema) {
    return schema.type() == Schema.Type.STRING &&
           schema.name() != null && schema.name().equals(AVRO_TYPE_ENUM);
  }

  private static boolean isInstanceOfAvroSchemaTypeForSimpleSchema(Schema fieldSchema,
                                                                   Object value) {
    List<Class> classes = SIMPLE_AVRO_SCHEMA_TYPES.get(fieldSchema.type());
    if (classes == null) {
      return false;
    }
    for (Class type : classes) {
      if (type.isInstance(value)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Split a full dotted-syntax name into a namespace and a single-component name.
   */
  private static String[] splitName(String fullName) {
    String[] result = new String[2];
    int indexLastDot = fullName.lastIndexOf('.');
    if (indexLastDot >= 0) {
      result[0] = fullName.substring(0, indexLastDot);
      result[1] = fullName.substring(indexLastDot+1);
    } else {
      result[0] = null;
      result[1] = fullName;
    }
    return result;
  }

  private interface LogicalTypeConverter {
    Object convert(Schema schema, Object value);
  }
}
