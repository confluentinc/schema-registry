/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.connect.avro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.connect.schema.ConnectEnum;
import io.confluent.connect.schema.ConnectUnion;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import org.apache.avro.AvroTypeException;
import org.apache.avro.JsonProperties;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.internal.JacksonUtils;
import org.apache.kafka.connect.data.ConnectSchema;
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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for converting between our runtime data format and Avro, and (de)serializing that data.
 */
public class AvroData {

  private static final Logger log = LoggerFactory.getLogger(AvroData.class);

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
  public static final String CONNECT_RECORD_DOC_PROP = "connect.record.doc";
  public static final String CONNECT_ENUM_DOC_PROP = "connect.enum.doc";
  public static final String CONNECT_VERSION_PROP = "connect.version";
  public static final String CONNECT_DEFAULT_VALUE_PROP = "connect.default";
  public static final String CONNECT_PARAMETERS_PROP = "connect.parameters";
  public static final String CONNECT_INTERNAL_TYPE_NAME = "connect.internal.type";
  public static final String AVRO_RECORD_DOC_PROP = NAMESPACE + ".record.doc";
  public static final String AVRO_ENUM_DOC_PREFIX_PROP = NAMESPACE + ".enum.doc.";
  public static final String AVRO_FIELD_DOC_PREFIX_PROP = NAMESPACE + ".field.doc.";
  //This property is used to determine whether a default value in the Connect schema originated
  //from an Avro field default
  public static final String AVRO_FIELD_DEFAULT_FLAG_PROP = NAMESPACE + ".field.default";
  public static final String AVRO_ENUM_DEFAULT_PREFIX_PROP = NAMESPACE + ".enum.default.";

  public static final String CONNECT_TYPE_PROP = "connect.type";

  public static final String CONNECT_TYPE_INT8 = "int8";
  public static final String CONNECT_TYPE_INT16 = "int16";

  public static final String AVRO_TYPE_UNION = NAMESPACE + ".Union";
  public static final String AVRO_TYPE_ENUM = NAMESPACE + ".Enum";

  public static final String AVRO_TYPE_ANYTHING = NAMESPACE + ".Anything";

  public static final String GENERALIZED_TYPE_UNION = ConnectUnion.LOGICAL_PARAMETER;
  public static final String GENERALIZED_TYPE_ENUM = ConnectEnum.LOGICAL_PARAMETER;
  public static final String GENERALIZED_TYPE_UNION_PREFIX = "connect_union_";
  public static final String GENERALIZED_TYPE_UNION_FIELD_PREFIX =
      GENERALIZED_TYPE_UNION_PREFIX + "field_";

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

  private static final org.apache.avro.Schema
      NULL_AVRO_SCHEMA =
      org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL);

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


  // Convert values in Connect form into their logical types. These logical converters are
  // discovered by logical type names specified in the field
  private static final HashMap<String, LogicalTypeConverter> TO_CONNECT_LOGICAL_CONVERTERS
      = new HashMap<>();

  static {
    TO_CONNECT_LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, new LogicalTypeConverter() {
      @Override
      public Object convert(Schema schema, Object value) {
        if (value instanceof byte[]) {
          return Decimal.toLogical(schema, (byte[]) value);
        } else if (value instanceof ByteBuffer) {
          return Decimal.toLogical(schema, ((ByteBuffer) value).array());
        }
        throw new DataException(
            "Invalid type for Decimal, underlying representation should be bytes but was "
            + value.getClass());
      }
    });

    TO_CONNECT_LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, new LogicalTypeConverter() {
      @Override
      public Object convert(Schema schema, Object value) {
        if (!(value instanceof Integer)) {
          throw new DataException(
              "Invalid type for Date, underlying representation should be int32 but was "
              + value.getClass());
        }
        return Date.toLogical(schema, (int) value);
      }
    });

    TO_CONNECT_LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, new LogicalTypeConverter() {
      @Override
      public Object convert(Schema schema, Object value) {
        if (!(value instanceof Integer)) {
          throw new DataException(
              "Invalid type for Time, underlying representation should be int32 but was "
              + value.getClass());
        }
        return Time.toLogical(schema, (int) value);
      }
    });

    TO_CONNECT_LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, new LogicalTypeConverter() {
      @Override
      public Object convert(Schema schema, Object value) {
        if (!(value instanceof Long)) {
          throw new DataException(
              "Invalid type for Timestamp, underlying representation should be int64 but was "
              + value.getClass());
        }
        return Timestamp.toLogical(schema, (long) value);
      }
    });
  }

  static final String AVRO_PROP = "avro";
  static final String AVRO_LOGICAL_TYPE_PROP = "logicalType";
  static final String AVRO_LOGICAL_TIMESTAMP_MILLIS = "timestamp-millis";
  static final String AVRO_LOGICAL_TIME_MILLIS = "time-millis";
  static final String AVRO_LOGICAL_DATE = "date";
  static final String AVRO_LOGICAL_DECIMAL = "decimal";
  static final String AVRO_LOGICAL_DECIMAL_SCALE_PROP = "scale";
  static final String AVRO_LOGICAL_DECIMAL_PRECISION_PROP = "precision";
  static final String CONNECT_AVRO_FIXED_SIZE_PROP = "connect.fixed.size";
  static final String CONNECT_AVRO_DECIMAL_PRECISION_PROP = "connect.decimal.precision";
  static final Integer CONNECT_AVRO_DECIMAL_PRECISION_DEFAULT = 64;

  private static final HashMap<String, LogicalTypeConverter> TO_AVRO_LOGICAL_CONVERTERS
      = new HashMap<>();

  static {
    TO_AVRO_LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, new LogicalTypeConverter() {
      @Override
      public Object convert(Schema schema, Object value) {
        if (!(value instanceof BigDecimal)) {
          throw new DataException(
              "Invalid type for Decimal, expected BigDecimal but was " + value.getClass());
        }
        return Decimal.fromLogical(schema, (BigDecimal) value);
      }
    });

    TO_AVRO_LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, new LogicalTypeConverter() {
      @Override
      public Object convert(Schema schema, Object value) {
        if (!(value instanceof java.util.Date)) {
          throw new DataException(
              "Invalid type for Date, expected Date but was " + value.getClass());
        }
        return Date.fromLogical(schema, (java.util.Date) value);
      }
    });

    TO_AVRO_LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, new LogicalTypeConverter() {
      @Override
      public Object convert(Schema schema, Object value) {
        if (!(value instanceof java.util.Date)) {
          throw new DataException(
              "Invalid type for Time, expected Date but was " + value.getClass());
        }
        return Time.fromLogical(schema, (java.util.Date) value);
      }
    });

    TO_AVRO_LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, new LogicalTypeConverter() {
      @Override
      public Object convert(Schema schema, Object value) {
        if (!(value instanceof java.util.Date)) {
          throw new DataException(
              "Invalid type for Timestamp, expected Date but was " + value.getClass());
        }
        return Timestamp.fromLogical(schema, (java.util.Date) value);
      }
    });
  }

  private int unionIndex = 0;

  private Map<Schema, org.apache.avro.Schema> fromConnectSchemaCache;
  private Map<AvroSchema, Schema> toConnectSchemaCache;
  private boolean connectMetaData;
  private boolean generalizedSumTypeSupport;
  private boolean ignoreDefaultForNullables;
  private boolean enhancedSchemaSupport;
  private boolean scrubInvalidNames;
  private boolean discardTypeDocDefault;
  private boolean allowOptionalMapKey;
  private boolean flattenSingletonUnion;

  public AvroData(int cacheSize) {
    this(new AvroDataConfig.Builder()
             .with(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, cacheSize)
             .build());
  }

  public AvroData(AvroDataConfig avroDataConfig) {
    fromConnectSchemaCache = new BoundedConcurrentHashMap<>(avroDataConfig.schemaCacheSize());
    toConnectSchemaCache = new BoundedConcurrentHashMap<>(avroDataConfig.schemaCacheSize());
    this.connectMetaData = avroDataConfig.isConnectMetaData();
    this.generalizedSumTypeSupport = avroDataConfig.isGeneralizedSumTypeSupport();
    this.ignoreDefaultForNullables = avroDataConfig.ignoreDefaultForNullables();
    this.enhancedSchemaSupport = avroDataConfig.isEnhancedAvroSchemaSupport();
    this.scrubInvalidNames = avroDataConfig.isScrubInvalidNames();
    this.discardTypeDocDefault = avroDataConfig.isDiscardTypeDocDefault();
    this.allowOptionalMapKey = avroDataConfig.isAllowOptionalMapKeys();
    this.flattenSingletonUnion = avroDataConfig.isFlattenSingletonUnion();
  }

  /**
   * Convert this object, in Connect data format, into an Avro object.
   */
  public Object fromConnectData(Schema schema, Object value) {
    org.apache.avro.Schema avroSchema = fromConnectSchema(schema);
    return fromConnectData(schema, avroSchema, value);
  }

  protected Object fromConnectData(Schema schema, org.apache.avro.Schema avroSchema, Object value) {
    return fromConnectData(schema, avroSchema, value, true, false);
  }

  /**
   * Convert from Connect data format to Avro. This version assumes the Avro schema has already
   * been converted and makes the use of NonRecordContainer optional
   *
   * @param schema                         the Connect schema
   * @param avroSchema                     the corresponding
   * @param logicalValue                   the Connect data to convert, which may be a value for
   *                                       a logical type
   * @param requireContainer               if true, wrap primitives, maps, and arrays in a
   *                                       NonRecordContainer before returning them
   * @param requireSchemalessContainerNull if true, use a container representation of null because
   *                                       this is part of struct/array/map and we cannot represent
   *                                       nulls as true null because Anything cannot be a union
   *                                       type; otherwise, this is a top-level value and can return
   *                                       null
   * @return the converted data
   */
  private Object fromConnectData(
      Schema schema,
      org.apache.avro.Schema avroSchema,
      Object logicalValue,
      boolean requireContainer,
      boolean requireSchemalessContainerNull
  ) {
    Schema.Type schemaType = schema != null
                             ? schema.type()
                             : schemaTypeForSchemalessJavaType(logicalValue);
    if (schemaType == null) {
      // Schemaless null data since schema is null and we got a null schema type from the value
      if (requireSchemalessContainerNull) {
        return new GenericRecordBuilder(ANYTHING_SCHEMA).build();
      } else {
        return null;
      }
    }

    validateSchemaValue(schema, logicalValue);

    if (logicalValue == null) {
      // But if this is schemaless, we may not be able to return null directly
      if (schema == null && requireSchemalessContainerNull) {
        return new GenericRecordBuilder(ANYTHING_SCHEMA).build();
      } else {
        return null;
      }
    }

    // If this is a logical type, convert it from the convenient Java type to the underlying
    // serializeable format
    Object value = logicalValue;
    if (schema != null && schema.name() != null) {
      LogicalTypeConverter logicalConverter = TO_AVRO_LOGICAL_CONVERTERS.get(schema.name());
      if (logicalConverter != null) {
        value = logicalConverter.convert(schema, logicalValue);
      }
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
          if (generalizedSumTypeSupport && ConnectEnum.isEnum(schema)) {
            String enumSchemaName = schema.parameters().get(GENERALIZED_TYPE_ENUM);
            value = enumSymbol(avroSchema, value, enumSchemaName);
          } else if (enhancedSchemaSupport && schema != null && schema.parameters() != null
              && schema.parameters().containsKey(AVRO_TYPE_ENUM)) {
            String enumSchemaName = schema.parameters().get(AVRO_TYPE_ENUM);
            value = enumSymbol(avroSchema, value, enumSchemaName);
          } else {
            String stringValue = (String) value; // Check for correct type
          }
          return maybeAddContainer(
              avroSchema,
              maybeWrapSchemaless(schema, value, ANYTHING_SCHEMA_STRING_FIELD),
              requireContainer);

        case BYTES: {
          value = value instanceof byte[] ? ByteBuffer.wrap((byte[]) value) :
                                  (ByteBuffer) value;
          if (schema != null && isFixedSchema(schema)) {
            int size = Integer.parseInt(schema.parameters().get(CONNECT_AVRO_FIXED_SIZE_PROP));
            org.apache.avro.Schema fixedSchema = null;
            if (avroSchema.getType() == org.apache.avro.Schema.Type.UNION) {
              int index = 0;
              for (org.apache.avro.Schema memberSchema : avroSchema.getTypes()) {
                if (memberSchema.getType() == org.apache.avro.Schema.Type.FIXED
                        && memberSchema.getFixedSize() == size
                        && unionMemberFieldName(memberSchema, index)
                           .equals(unionMemberFieldName(schema, index))) {
                  fixedSchema = memberSchema;
                }
                index++;
              }
              if (fixedSchema == null) {
                throw new DataException("Fixed size " + size + " not in union " + avroSchema);
              }
            } else {
              fixedSchema = avroSchema;
            }
            value = new GenericData.Fixed(fixedSchema, ((ByteBuffer)value).array());
          }
          return maybeAddContainer(
              avroSchema,
              maybeWrapSchemaless(schema, value, ANYTHING_SCHEMA_BYTES_FIELD),
              requireContainer);
        }

        case ARRAY: {
          Collection<Object> list = (Collection<Object>) value;
          // TODO most types don't need a new converted object since types pass through
          List<Object> converted = new ArrayList<>(list.size());
          Schema elementSchema = schema != null ? schema.valueSchema() : null;
          org.apache.avro.Schema underlyingAvroSchema = avroSchemaForUnderlyingTypeIfOptional(
              schema, avroSchema, scrubInvalidNames);
          org.apache.avro.Schema elementAvroSchema =
              schema != null ? underlyingAvroSchema.getElementType() : ANYTHING_SCHEMA;
          for (Object val : list) {
            converted.add(
                fromConnectData(
                    elementSchema,
                    elementAvroSchema,
                    val,
                    false,
                    true
                )
            );
          }
          return maybeAddContainer(
              avroSchema,
              maybeWrapSchemaless(schema, converted, ANYTHING_SCHEMA_ARRAY_FIELD),
              requireContainer);
        }

        case MAP: {
          Map<Object, Object> map = (Map<Object, Object>) value;
          org.apache.avro.Schema underlyingAvroSchema;
          if (schema != null && schema.keySchema().type() == Schema.Type.STRING
              && (!schema.keySchema().isOptional() || allowOptionalMapKey)) {

            // TODO most types don't need a new converted object since types pass through
            underlyingAvroSchema = avroSchemaForUnderlyingTypeIfOptional(
                schema, avroSchema, scrubInvalidNames);
            Map<String, Object> converted = new HashMap<>();
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
              // Key is a String, no conversion needed
              Object convertedValue = fromConnectData(schema.valueSchema(),
                  underlyingAvroSchema.getValueType(),
                  entry.getValue(), false, true
              );
              converted.put((String) entry.getKey(), convertedValue);
            }
            return maybeAddContainer(avroSchema, converted, requireContainer);
          } else {
            List<GenericRecord> converted = new ArrayList<>(map.size());
            underlyingAvroSchema = avroSchemaForUnderlyingMapEntryType(schema, avroSchema);
            org.apache.avro.Schema elementSchema =
                schema != null
                ? underlyingAvroSchema.getElementType()
                : ANYTHING_SCHEMA_MAP_ELEMENT;
            org.apache.avro.Schema avroKeySchema = elementSchema.getField(KEY_FIELD).schema();
            org.apache.avro.Schema avroValueSchema = elementSchema.getField(VALUE_FIELD).schema();
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
              Object keyConverted = fromConnectData(schema != null ? schema.keySchema() : null,
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
          if (!struct.schema().equals(schema)) {
            throw new DataException("Mismatching struct schema");
          }
          //This handles the inverting of a union which is held as a struct, where each field is
          // one of the union types.
          if (isUnionSchema(schema)) {
            for (Field field : schema.fields()) {
              Object object = ignoreDefaultForNullables
                  ? struct.getWithoutDefault(field.name()) : struct.get(field);
              if (object != null) {
                return fromConnectData(
                    field.schema(),
                    avroSchema,
                    object,
                    false,
                    true
                );
              }
            }
            return fromConnectData(schema, avroSchema, null, false, true);
          } else {
            org.apache.avro.Schema underlyingAvroSchema = avroSchemaForUnderlyingTypeIfOptional(
                schema, avroSchema, scrubInvalidNames);
            GenericRecordBuilder convertedBuilder = new GenericRecordBuilder(underlyingAvroSchema);
            for (Field field : schema.fields()) {
              String fieldName = scrubName(field.name(), scrubInvalidNames);
              org.apache.avro.Schema.Field theField = underlyingAvroSchema.getField(fieldName);
              org.apache.avro.Schema fieldAvroSchema = theField.schema();
              Object fieldValue = ignoreDefaultForNullables
                  ? struct.getWithoutDefault(field.name()) : struct.get(field);
              convertedBuilder.set(
                  fieldName,
                  fromConnectData(field.schema(), fieldAvroSchema, fieldValue, false, true)
              );
            }
            return convertedBuilder.build();
          }
        }

        default:
          throw new DataException("Unknown schema type: " + schema.type());
      }
    } catch (ClassCastException e) {
      throw new DataException("Invalid type for " + schema.type() + ": " + value.getClass());
    }
  }

  private EnumSymbol enumSymbol(
      org.apache.avro.Schema avroSchema, Object value, String enumSchemaName) {
    org.apache.avro.Schema enumSchema;
    if (avroSchema.getType() == org.apache.avro.Schema.Type.UNION) {
      int enumIndex = avroSchema.getIndexNamed(enumSchemaName);
      enumSchema = avroSchema.getTypes().get(enumIndex);
    } else {
      enumSchema = avroSchema;
    }
    return new GenericData.EnumSymbol(enumSchema, (String) value);
  }

  /**
   * MapEntry types in connect Schemas are represented as Arrays of record.
   * Return the array type from the union instead of the union itself.
   */
  private static org.apache.avro.Schema avroSchemaForUnderlyingMapEntryType(
      Schema schema,
      org.apache.avro.Schema avroSchema) {

    if (schema != null && schema.isOptional()) {
      if (avroSchema.getType() == org.apache.avro.Schema.Type.UNION) {
        for (org.apache.avro.Schema typeSchema : avroSchema.getTypes()) {
          if (!typeSchema.getType().equals(org.apache.avro.Schema.Type.NULL)
              && Schema.Type.ARRAY.getName().equals(typeSchema.getType().getName())) {
            return typeSchema;
          }
        }
      } else {
        throw new DataException(
            "An optional schema should have an Avro Union type, not "
            + schema.type());
      }
    }
    return avroSchema;
  }

  private static boolean crossReferenceSchemaNames(final Schema schema,
                                                   final org.apache.avro.Schema avroSchema,
                                                   final boolean scrubInvalidNames) {
    String fullName = scrubFullName(schema.name(), scrubInvalidNames);
    return Objects.equals(avroSchema.getFullName(), fullName)
        || Objects.equals(avroSchema.getType().getName(), schema.type().getName())
        || (schema.name() == null && avroSchema.getFullName().startsWith(DEFAULT_SCHEMA_FULL_NAME));
  }

  /**
   * Connect optional fields are represented as a unions (null & type) in Avro
   * Return the Avro schema of the actual type in the Union (instead of the union itself)
   */
  private static org.apache.avro.Schema avroSchemaForUnderlyingTypeIfOptional(
      Schema schema, org.apache.avro.Schema avroSchema, boolean scrubInvalidNames) {

    if (schema != null && schema.isOptional()) {
      if (avroSchema.getType() == org.apache.avro.Schema.Type.UNION) {
        for (org.apache.avro.Schema typeSchema : avroSchema
            .getTypes()) {
          if (!typeSchema.getType().equals(org.apache.avro.Schema.Type.NULL)
              && crossReferenceSchemaNames(schema, typeSchema, scrubInvalidNames)) {
            return typeSchema;
          }
        }
      } else {
        throw new DataException(
            "An optional schema should have an Avro Union type, not "
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
    return fromConnectSchema(schema, new HashMap<Schema, org.apache.avro.Schema>());
  }

  public org.apache.avro.Schema fromConnectSchema(Schema schema,
                                                  Map<Schema, org.apache.avro.Schema> schemaMap) {
    if (schema == null) {
      return ANYTHING_SCHEMA;
    }

    org.apache.avro.Schema cached = fromConnectSchemaCache.get(schema);
    if (cached != null) {
      return cached;
    }

    FromConnectContext fromConnectContext = new FromConnectContext(schemaMap);
    org.apache.avro.Schema finalSchema = fromConnectSchema(schema, fromConnectContext, false);
    fromConnectSchemaCache.put(schema, finalSchema);
    return finalSchema;
  }

  /**
   * SchemaMap is a map of already resolved internal schemas, this avoids type re-declaration if a
   * type is reused, this actually blows up if you don't do this and have a type used in multiple
   * places.
   *
   * <p>Also it only holds reference the non-optional schemas as technically an optional is
   * actually a union of null and the non-opitonal, which if used in multiple places some optional
   * some non-optional will cause error as you redefine type.
   *
   * <p>This is different to the global schema cache which is used to hold/cache fully resolved
   * schemas used to avoid re-resolving when presented with the same source schema.
   */
  public org.apache.avro.Schema fromConnectSchema(Schema schema,
                                                  FromConnectContext fromConnectContext,
                                                  boolean ignoreOptional) {
    if (schema == null) {
      return ANYTHING_SCHEMA;
    }

    if (!isUnionSchema(schema) && !schema.isOptional()) {
      org.apache.avro.Schema cached = fromConnectContext.schemaMap.get(schema);
      if (cached != null) {
        return cached;
      }
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
        if ((generalizedSumTypeSupport || enhancedSchemaSupport)
            && schema.parameters() != null
            && (schema.parameters().containsKey(GENERALIZED_TYPE_ENUM)
                || schema.parameters().containsKey(AVRO_TYPE_ENUM))) {
          String paramName = generalizedSumTypeSupport ? GENERALIZED_TYPE_ENUM : AVRO_TYPE_ENUM;
          List<String> symbols = new ArrayList<>();
          for (Map.Entry<String, String> entry : schema.parameters().entrySet()) {
            if (entry.getKey().startsWith(paramName + ".")) {
              String enumSymbol = entry.getKey().substring(paramName.length() + 1);
              symbols.add(enumSymbol);
            }
          }
          Pair<String, String> names = getNameOrDefault(fromConnectContext, schema.name());
          String name = names.getValue();
          String enumName = schema.parameters().get(paramName);
          String enumDoc = schema.parameters().get(AVRO_ENUM_DOC_PREFIX_PROP + name);
          String enumDefault = schema.parameters().get(AVRO_ENUM_DEFAULT_PREFIX_PROP + name);
          baseSchema = discardTypeDocDefault
              ? org.apache.avro.SchemaBuilder.builder().enumeration(enumName)
                        .doc(schema.parameters().get(CONNECT_ENUM_DOC_PROP))
                        .symbols(symbols.toArray(new String[symbols.size()]))
              : org.apache.avro.SchemaBuilder.builder().enumeration(enumName)
                        .doc(enumDoc)
                        .defaultSymbol(enumDefault)
                        .symbols(symbols.toArray(new String[symbols.size()]));
        } else {
          baseSchema = org.apache.avro.SchemaBuilder.builder().stringType();
        }
        break;
      case BYTES:
        if (isFixedSchema(schema)) {
          Pair<String, String> names = getNameOrDefault(fromConnectContext, schema.name());
          String namespace = names.getKey();
          String name = names.getValue();
          baseSchema = org.apache.avro.SchemaBuilder.builder()
                  .fixed(name)
                  .namespace(namespace)
                  .size(Integer.parseInt(schema.parameters().get(CONNECT_AVRO_FIXED_SIZE_PROP)));
        } else {
          baseSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
        }
        if (Decimal.LOGICAL_NAME.equalsIgnoreCase(schema.name())) {
          int scale = Integer.parseInt(schema.parameters().get(Decimal.SCALE_FIELD));
          baseSchema.addProp(AVRO_LOGICAL_DECIMAL_SCALE_PROP, new IntNode(scale));
          if (schema.parameters().containsKey(CONNECT_AVRO_DECIMAL_PRECISION_PROP)) {
            String precisionValue = schema.parameters().get(CONNECT_AVRO_DECIMAL_PRECISION_PROP);
            int precision = Integer.parseInt(precisionValue);
            baseSchema.addProp(AVRO_LOGICAL_DECIMAL_PRECISION_PROP, new IntNode(precision));
          } else {
            baseSchema
                .addProp(AVRO_LOGICAL_DECIMAL_PRECISION_PROP,
                         new IntNode(CONNECT_AVRO_DECIMAL_PRECISION_DEFAULT));
          }
        }
        break;
      case ARRAY:
        baseSchema = org.apache.avro.SchemaBuilder.builder().array()
            .items(fromConnectSchemaWithCycle(schema.valueSchema(), fromConnectContext, false));
        break;
      case MAP:
        // Avro only supports string keys, so we match the representation when possible, but
        // otherwise fall back on a record representation
        if (schema.keySchema().type() == Schema.Type.STRING
            && (!schema.keySchema().isOptional() || allowOptionalMapKey)) {
          baseSchema = org.apache.avro.SchemaBuilder.builder().map().values(
              fromConnectSchemaWithCycle(schema.valueSchema(), fromConnectContext, false));
        } else {
          // Special record name indicates format
          List<org.apache.avro.Schema.Field> fields = new ArrayList<>();
          final org.apache.avro.Schema mapSchema;
          if (schema.name() == null) {
            mapSchema = org.apache.avro.Schema.createRecord(
                MAP_ENTRY_TYPE_NAME,
                null,
                NAMESPACE,
                false
            );
          } else {
            Pair<String, String> names = getNameOrDefault(fromConnectContext, schema.name());
            String namespace = names.getKey();
            String name = names.getValue();
            mapSchema = org.apache.avro.Schema.createRecord(name, null, namespace, false);
            mapSchema.addProp(CONNECT_INTERNAL_TYPE_NAME, MAP_ENTRY_TYPE_NAME);
          }
          addAvroRecordField(
              fields,
              KEY_FIELD,
              schema.keySchema(),
              null,
              fromConnectContext);
          addAvroRecordField(
              fields,
              VALUE_FIELD,
              schema.valueSchema(),
              null,
              fromConnectContext);
          mapSchema.setFields(fields);
          baseSchema = org.apache.avro.Schema.createArray(mapSchema);
        }
        break;
      case STRUCT:
        if (isUnionSchema(schema)) {
          List<org.apache.avro.Schema> unionSchemas = new ArrayList<>();
          if (schema.isOptional()) {
            unionSchemas.add(org.apache.avro.SchemaBuilder.builder().nullType());
          }
          for (Field field : schema.fields()) {
            unionSchemas.add(
                fromConnectSchemaWithCycle(nonOptional(field.schema()), fromConnectContext, true));
          }
          baseSchema = org.apache.avro.Schema.createUnion(unionSchemas);
        } else if (schema.isOptional()) {
          List<org.apache.avro.Schema> unionSchemas = new ArrayList<>();
          unionSchemas.add(org.apache.avro.SchemaBuilder.builder().nullType());
          unionSchemas.add(
              fromConnectSchemaWithCycle(nonOptional(schema), fromConnectContext, false));
          baseSchema = org.apache.avro.Schema.createUnion(unionSchemas);
        } else {
          Pair<String, String> names = getNameOrDefault(fromConnectContext, schema.name());
          String namespace = names.getKey();
          String name = names.getValue();
          String doc = schema.parameters() != null
                       ? schema.parameters()
                       .get(discardTypeDocDefault ? CONNECT_RECORD_DOC_PROP : AVRO_RECORD_DOC_PROP)
                       : null;
          baseSchema = org.apache.avro.Schema.createRecord(name, doc, namespace, false);
          if (schema.name() != null) {
            fromConnectContext.cycleReferences.put(schema.name(), baseSchema);
          }
          List<org.apache.avro.Schema.Field> fields = new ArrayList<>();
          for (Field field : schema.fields()) {
            String fieldName = scrubName(field.name());
            String fieldDoc = null;
            if (!discardTypeDocDefault && schema.parameters() != null) {
              fieldDoc = schema.parameters().get(AVRO_FIELD_DOC_PREFIX_PROP + field.name());
            }
            addAvroRecordField(fields, fieldName, field.schema(), fieldDoc, fromConnectContext);
          }
          baseSchema.setFields(fields);
        }
        break;
      default:
        throw new DataException("Unknown schema type: " + schema.type());
    }

    org.apache.avro.Schema finalSchema = baseSchema;
    if (!baseSchema.getType().equals(org.apache.avro.Schema.Type.UNION)) {
      if (connectMetaData) {
        if (schema.doc() != null) {
          baseSchema.addProp(CONNECT_DOC_PROP, schema.doc());
        }
        if (schema.version() != null) {
          baseSchema.addProp(CONNECT_VERSION_PROP,
                             JsonNodeFactory.instance.numberNode(schema.version()));
        }
        if (schema.parameters() != null) {
          JsonNode params = parametersFromConnect(schema.parameters());
          if (!params.isEmpty()) {
            baseSchema.addProp(CONNECT_PARAMETERS_PROP, params);
          }
        }
        if (schema.defaultValue() != null) {
          if (discardTypeDocDefault || schema.parameters() == null
              || !schema.parameters().containsKey(AVRO_FIELD_DEFAULT_FLAG_PROP)) {
            baseSchema.addProp(CONNECT_DEFAULT_VALUE_PROP,
                defaultValueFromConnect(schema, schema.defaultValue()));
          }
        }
        if (schema.name() != null) {
          baseSchema.addProp(CONNECT_NAME_PROP, schema.name());
        }
        // Some Connect types need special annotations to preserve the types accurate due to
        // limitations in Avro. These types get an extra annotation with their Connect type
        if (connectType != null) {
          baseSchema.addProp(CONNECT_TYPE_PROP, connectType);
        }
      }

      boolean forceLegacyDecimal = false;
      // the new and correct way to handle logical types
      if (schema.name() != null) {
        if (Decimal.LOGICAL_NAME.equalsIgnoreCase(schema.name())) {
          String precisionString = schema.parameters().get(CONNECT_AVRO_DECIMAL_PRECISION_PROP);
          String scaleString = schema.parameters().get(Decimal.SCALE_FIELD);
          int precision = precisionString == null ? CONNECT_AVRO_DECIMAL_PRECISION_DEFAULT :
              Integer.parseInt(precisionString);
          int scale = scaleString == null ? 0 : Integer.parseInt(scaleString);
          if (scale < 0 || scale > precision) {
            log.trace(
                "Scale and precision of {} and {} cannot be serialized as native Avro logical "
                    + "decimal type; reverting to legacy serialization method",
                scale,
                precision
            );
            // We cannot use the Avro Java library's support for the decimal logical type when the
            // scale is either negative or greater than the precision as this violates the Avro spec
            // and causes the Avro library to throw an exception, so we fall back in this case to
            // using the legacy method for encoding decimal logical type information.
            // Can't add a key/value pair with the CONNECT_AVRO_DECIMAL_PRECISION_PROP key to the
            // schema's parameters since the parameters for Connect schemas are immutable, so we
            // just track this in a local boolean variable instead.
            forceLegacyDecimal = true;
          } else {
            org.apache.avro.LogicalTypes.decimal(precision, scale).addToSchema(baseSchema);
          }
        } else if (Time.LOGICAL_NAME.equalsIgnoreCase(schema.name())) {
          org.apache.avro.LogicalTypes.timeMillis().addToSchema(baseSchema);
        } else if (Timestamp.LOGICAL_NAME.equalsIgnoreCase(schema.name())) {
          org.apache.avro.LogicalTypes.timestampMillis().addToSchema(baseSchema);
        } else if (Date.LOGICAL_NAME.equalsIgnoreCase(schema.name())) {
          org.apache.avro.LogicalTypes.date().addToSchema(baseSchema);
        }
      }

      // Initially, to add support for logical types a new property was added
      // with key `logicalType`. This enabled logical types for avro schemas but not others,
      // such as parquet. The use of 'addToSchema` above supersedes this method here,
      //  which should eventually be removed.
      // Keeping for backwards compatibility until a major version upgrade happens.

      // Below follows the older method of supporting logical types via properties.
      // It is retained for now and will be deprecated eventually.
      // Only Avro named types (record, enum, fixed) may contain namespace + name. Only Connect's
      // struct converts to one of those (record), so for everything else that has a name we store
      // the full name into a special property. For uniformity, we also duplicate this info into
      // the same field in records as well even though it will also be available in the namespace()
      // and name().
      if (schema.name() != null) {
        if (Decimal.LOGICAL_NAME.equalsIgnoreCase(schema.name())
            && (schema.parameters().containsKey(CONNECT_AVRO_DECIMAL_PRECISION_PROP)
                || forceLegacyDecimal)) {
          baseSchema.addProp(AVRO_LOGICAL_TYPE_PROP, AVRO_LOGICAL_DECIMAL);
        } else if (Time.LOGICAL_NAME.equalsIgnoreCase(schema.name())) {
          baseSchema.addProp(AVRO_LOGICAL_TYPE_PROP, AVRO_LOGICAL_TIME_MILLIS);
        } else if (Timestamp.LOGICAL_NAME.equalsIgnoreCase(schema.name())) {
          baseSchema.addProp(AVRO_LOGICAL_TYPE_PROP, AVRO_LOGICAL_TIMESTAMP_MILLIS);
        } else if (Date.LOGICAL_NAME.equalsIgnoreCase(schema.name())) {
          baseSchema.addProp(AVRO_LOGICAL_TYPE_PROP, AVRO_LOGICAL_DATE);
        }
      }

      if (schema.parameters() != null) {
        for (Map.Entry<String, String> entry : schema.parameters().entrySet()) {
          if (entry.getKey().startsWith(AVRO_PROP)) {
            baseSchema.addProp(entry.getKey(), entry.getValue());
          }
        }
      }

      // Note that all metadata has already been processed and placed on the baseSchema because we
      // can't store any metadata on the actual top-level schema when it's a union because of Avro
      // constraints on the format of schemas.
      if (!ignoreOptional) {
        finalSchema = maybeMakeOptional(schema, baseSchema);
      }
    }

    if (!schema.isOptional()) {
      fromConnectContext.schemaMap.put(schema, finalSchema);
    }
    return finalSchema;
  }

  private Pair<String, String> getNameOrDefault(FromConnectContext ctx, String name) {
    if (name != null) {
      String[] split = splitName(name);
      return new Pair<>(split[0], split[1]);
    } else {
      int nameIndex = ctx.incrementAndGetNameIndex();
      return new Pair<>(NAMESPACE, DEFAULT_SCHEMA_NAME + (nameIndex > 1 ? nameIndex : ""));
    }
  }

  private org.apache.avro.Schema maybeMakeOptional(
      Schema schema, org.apache.avro.Schema baseSchema) {
    if (!schema.isOptional()) {
      return baseSchema;
    }
    if (schema.defaultValue() != null) {
      return org.apache.avro.SchemaBuilder.builder().unionOf()
          .type(baseSchema).and()
          .nullType()
          .endUnion();
    } else {
      return org.apache.avro.SchemaBuilder.builder().unionOf()
          .nullType().and()
          .type(baseSchema)
          .endUnion();
    }
  }

  private static String scrubFullName(String name, boolean scrubInvalidNames) {
    if (name == null || !scrubInvalidNames) {
      return name;
    }
    String[] split = splitName(name, scrubInvalidNames);
    if (split[0] == null) {
      return split[1];
    } else {
      return split[0] + "." + split[1];
    }
  }

  private String scrubName(String name) {
    return scrubName(name, scrubInvalidNames);
  }

  private static String scrubName(String name, boolean scrubInvalidNames) {
    return scrubInvalidNames ? doScrubName(name) : name;
  }

  // Visible for testing
  protected static String doScrubName(String name) {
    try {
      if (name == null || name.isEmpty()) {
        return name;
      }

      // This function was originally written more simply using regular expressions, but this was
      // observed to significantly cut performance by half when locally sourcing data:
      // https://github.com/confluentinc/schema-registry/issues/2929

      // Fast code path for returning: avoids making a single memory allocation if the name does
      // not need to be modified.
      boolean nameOK = true;
      for (int i = 0, n = name.length(); i < n; i++) {
        char ch = name.charAt(i);
        if ((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')
                || (ch >= '0' && ch <= '9') || ch == '_') {
          continue;
        }
        nameOK = false;
      }
      nameOK = nameOK && (name.charAt(0) < '0' || name.charAt(0) > '9');
      if (nameOK) {
        return name;
      }

      // String needs to be scrubbed
      String encoded = URLEncoder.encode(name, "UTF-8");
      if (encoded.charAt(0) >= '0' && encoded.charAt(0) <= '9') {
        encoded = "x" + encoded;  // use an arbitrary valid prefix
      }
      StringBuilder sb = new StringBuilder(encoded);
      for (int i = 0, n = sb.length(); i < n; i++) {
        char ch = sb.charAt(i);
        if ((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')
                || (ch >= '0' && ch <= '9') || ch == '_') {
          continue;
        }
        sb.setCharAt(i, '_');
      }
      return sb.toString();
    } catch (UnsupportedEncodingException e) {
      return name;
    }
  }

  public org.apache.avro.Schema fromConnectSchemaWithCycle(
      Schema schema,
      FromConnectContext fromConnectContext, boolean ignoreOptional) {
    org.apache.avro.Schema resolvedSchema;
    if (fromConnectContext.cycleReferences.containsKey(schema.name())) {
      resolvedSchema = fromConnectContext.cycleReferences.get(schema.name());
      if (!ignoreOptional) {
        resolvedSchema = maybeMakeOptional(schema, resolvedSchema);
      }
    } else {
      resolvedSchema = fromConnectSchema(schema, fromConnectContext, ignoreOptional);
    }
    return resolvedSchema;
  }

  private void addAvroRecordField(
      List<org.apache.avro.Schema.Field> fields,
      String fieldName, Schema fieldSchema,
      String fieldDoc,
      FromConnectContext fromConnectContext) {

    Object defaultVal = null;
    if (fieldSchema.defaultValue() != null) {
      defaultVal = JacksonUtils.toObject(
          defaultValueFromConnect(fieldSchema, fieldSchema.defaultValue()));
    } else if (fieldSchema.isOptional()) {
      defaultVal = JsonProperties.NULL_VALUE;
    }
    org.apache.avro.Schema.Field field;
    org.apache.avro.Schema schema = fromConnectSchema(fieldSchema, fromConnectContext, false);
    try {
      field = new org.apache.avro.Schema.Field(
          fieldName,
          schema,
          discardTypeDocDefault ? fieldSchema.doc() : fieldDoc,
          defaultVal);
    } catch (AvroTypeException e) {
      field = new org.apache.avro.Schema.Field(
          fieldName,
          schema,
          discardTypeDocDefault ? fieldSchema.doc() : fieldDoc);
      log.warn("Ignoring invalid default for field {}", fieldName, e);
    }
    fields.add(field);
  }


  private static Object toAvroLogical(Schema schema, Object value) {
    if (schema != null && schema.name() != null) {
      LogicalTypeConverter logicalConverter = TO_AVRO_LOGICAL_CONVERTERS.get(schema.name());
      if (logicalConverter != null && value != null) {
        return logicalConverter.convert(schema, value);
      }
    }
    return value;
  }

  private static Object toConnectLogical(Schema schema, Object value) {
    if (schema != null && schema.name() != null) {
      LogicalTypeConverter logicalConverter = TO_CONNECT_LOGICAL_CONVERTERS.get(schema.name());
      if (logicalConverter != null && value != null) {
        return logicalConverter.convert(schema, value);
      }
    }
    return value;
  }

  // Convert default values from Connect data format to Avro's format, which is an
  // org.codehaus.jackson.JsonNode. The default value is provided as an argument because even
  // though you can get a default value from the schema, default values for complex structures need
  // to perform the same translation but those defaults will be part of the original top-level
  // (complex type) default value, not part of the child schema.
  private JsonNode defaultValueFromConnect(Schema schema, Object value) {
    try {
      if (value == null) {
        return NullNode.getInstance();
      }

      // If this is a logical type, convert it from the convenient Java type to the underlying
      // serializeable format
      Object defaultVal = toAvroLogical(schema, value);

      switch (schema.type()) {
        case INT8:
          return JsonNodeFactory.instance.numberNode(((Byte) defaultVal).intValue());
        case INT16:
          return JsonNodeFactory.instance.numberNode(((Short) defaultVal).intValue());
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
          if (defaultVal instanceof byte[]) {
            return JsonNodeFactory.instance.textNode(new String((byte[]) defaultVal,
                StandardCharsets.ISO_8859_1));
          } else {
            return JsonNodeFactory.instance.textNode(new String(((ByteBuffer) defaultVal).array(),
                StandardCharsets.ISO_8859_1));
          }
        case ARRAY: {
          ArrayNode array = JsonNodeFactory.instance.arrayNode();
          for (Object elem : (Collection<Object>) defaultVal) {
            array.add(defaultValueFromConnect(schema.valueSchema(), elem));
          }
          return array;
        }
        case MAP:
          if (schema.keySchema().type() == Schema.Type.STRING
              && (!schema.keySchema().isOptional() || allowOptionalMapKey)) {
            ObjectNode node = JsonNodeFactory.instance.objectNode();
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) defaultVal).entrySet()) {
              JsonNode entryDef = defaultValueFromConnect(schema.valueSchema(), entry.getValue());
              node.put(entry.getKey(), entryDef);
            }
            return node;
          } else {
            ArrayNode array = JsonNodeFactory.instance.arrayNode();
            for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) defaultVal).entrySet()) {
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
          boolean isUnion = isUnionSchema(schema);
          ObjectNode node = JsonNodeFactory.instance.objectNode();
          Struct struct = ((Struct) defaultVal);
          for (Field field : (schema.fields())) {
            String fieldName = scrubName(field.name());
            JsonNode fieldDef = defaultValueFromConnect(field.schema(), struct.get(field));
            if (isUnion) {
              return fieldDef;
            }
            node.put(fieldName, fieldDef);
          }
          return node;
        }
        default:
          throw new DataException("Unknown schema type:" + schema.type());
      }
    } catch (ClassCastException e) {
      throw new DataException("Invalid type used for default value of "
                              + schema.type()
                              + " field: "
                              + schema.defaultValue().getClass());
    }
  }


  private JsonNode parametersFromConnect(Map<String, String> params) {
    ObjectNode result = JsonNodeFactory.instance.objectNode();
    for (Map.Entry<String, String> entry : params.entrySet()) {
      if (discardTypeDocDefault || !entry.getKey().equals(AVRO_FIELD_DEFAULT_FLAG_PROP)) {
        result.put(entry.getKey(), entry.getValue());
      }
    }
    return result;
  }

  private static void validateSchemaValue(Schema schema, Object value) throws DataException {
    if (value == null && schema != null && !schema.isOptional()) {
      throw new DataException("Found null value for non-optional schema");
    }
  }

  private boolean isMapEntry(final org.apache.avro.Schema elemSchema) {
    if (!elemSchema.getType().equals(org.apache.avro.Schema.Type.RECORD)) {
      return false;
    }
    if (NAMESPACE.equals(elemSchema.getNamespace())
        && MAP_ENTRY_TYPE_NAME.equals(elemSchema.getName())) {
      return true;
    }
    if (Objects.equals(elemSchema.getProp(CONNECT_INTERNAL_TYPE_NAME), MAP_ENTRY_TYPE_NAME)) {
      return true;
    }
    return false;
  }

  /**
   * Convert the given object, in Avro format, into a Connect data object.
   * @param avroSchema the Avro schema
   * @param value the value to convert into a Connect data object
   * @return the Connect schema and value
   */
  public SchemaAndValue toConnectData(org.apache.avro.Schema avroSchema, Object value) {
    return toConnectData(avroSchema, value, null);
  }

  /**
   * Convert the given object, in Avro format, into a Connect data object.
   * @param avroSchema the Avro schema
   * @param value the value to convert into a Connect data object
   * @param version the version to set on the Connect schema if the avroSchema does not have a
   *     property named "connect.version", may be null
   * @return the Connect schema and value
   */
  public SchemaAndValue toConnectData(org.apache.avro.Schema avroSchema, Object value,
                                      Integer version) {
    if (value == null) {
      return null;
    }
    ToConnectContext toConnectContext = new ToConnectContext();
    Schema schema = (avroSchema.equals(ANYTHING_SCHEMA))
                    ? null
                    : toConnectSchema(avroSchema, version, toConnectContext);
    return new SchemaAndValue(schema, toConnectData(schema, value, toConnectContext));
  }

  private Object toConnectData(Schema schema, Object value, ToConnectContext toConnectContext) {
    return toConnectData(schema, value, toConnectContext, true);
  }

  private Object toConnectData(Schema schema, Object value, ToConnectContext toConnectContext,
                               boolean doLogicalConversion) {
    validateSchemaValue(schema, value);
    if (value == null || value == JsonProperties.NULL_VALUE) {
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
        if (boolVal != null) {
          return toConnectData(Schema.BOOLEAN_SCHEMA, boolVal, toConnectContext);
        }

        Object
            bytesVal =
            recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_BYTES_FIELD).pos());
        if (bytesVal != null) {
          return toConnectData(Schema.BYTES_SCHEMA, bytesVal, toConnectContext);
        }

        Object
            dblVal =
            recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_DOUBLE_FIELD).pos());
        if (dblVal != null) {
          return toConnectData(Schema.FLOAT64_SCHEMA, dblVal, toConnectContext);
        }

        Object
            fltVal =
            recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_FLOAT_FIELD).pos());
        if (fltVal != null) {
          return toConnectData(Schema.FLOAT32_SCHEMA, fltVal, toConnectContext);
        }

        Object intVal = recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_INT_FIELD).pos());
        if (intVal != null) {
          return toConnectData(Schema.INT32_SCHEMA, intVal, toConnectContext);
        }

        Object
            longVal =
            recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_LONG_FIELD).pos());
        if (longVal != null) {
          return toConnectData(Schema.INT64_SCHEMA, longVal, toConnectContext);
        }

        Object
            stringVal =
            recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_STRING_FIELD).pos());
        if (stringVal != null) {
          return toConnectData(Schema.STRING_SCHEMA, stringVal, toConnectContext);
        }

        Object
            arrayVal =
            recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_ARRAY_FIELD).pos());
        if (arrayVal != null) {
          // We cannot reuse the logic like we do in other cases because it is not possible to
          // construct an array schema with a null item schema, but the items have no schema.
          if (!(arrayVal instanceof Collection)) {
            throw new DataException(
                "Expected a Collection for schemaless array field but found a "
                + arrayVal.getClass().getName()
            );
          }
          Collection<Object> original = (Collection<Object>) arrayVal;
          List<Object> result = new ArrayList<>(original.size());
          for (Object elem : original) {
            result.add(toConnectData((Schema) null, elem, toConnectContext));
          }
          return result;
        }

        Object mapVal = recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_MAP_FIELD).pos());
        if (mapVal != null) {
          // We cannot reuse the logic like we do in other cases because it is not possible to
          // construct a map schema with a null item schema, but the items have no schema.
          if (!(mapVal instanceof Collection)) {
            throw new DataException(
                "Expected a List for schemaless map field but found a "
                + mapVal.getClass().getName()
            );
          }
          Collection<IndexedRecord> original = (Collection<IndexedRecord>) mapVal;
          Map<Object, Object> result = new HashMap<>(original.size());
          for (IndexedRecord entry : original) {
            int avroKeyFieldIndex = entry.getSchema().getField(KEY_FIELD).pos();
            int avroValueFieldIndex = entry.getSchema().getField(VALUE_FIELD).pos();
            Object convertedKey = toConnectData(
                null, entry.get(avroKeyFieldIndex), toConnectContext);
            Object convertedValue = toConnectData(
                null, entry.get(avroValueFieldIndex), toConnectContext);
            result.put(convertedKey, convertedValue);
          }
          return result;
        }

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
          converted = ((Integer) value).byteValue();
          break;
        case INT16:
          // Encoded as an Integer
          converted = ((Integer) value).shortValue();
          break;

        case STRING:
          if (value instanceof String) {
            converted = value;
          } else if (value instanceof CharSequence
                     || value instanceof GenericEnumSymbol
                     || value instanceof Enum) {
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
            result.add(toConnectData(valueSchema, elem, toConnectContext));
          }
          converted = result;
          break;
        }

        case MAP: {
          Schema keySchema = schema.keySchema();
          Schema valueSchema = schema.valueSchema();
          if (keySchema != null && keySchema.type() == Schema.Type.STRING && !keySchema
              .isOptional()) {
            // Non-optional string keys
            Map<CharSequence, Object> original = (Map<CharSequence, Object>) value;
            Map<CharSequence, Object> result = new HashMap<>(original.size());
            for (Map.Entry<CharSequence, Object> entry : original.entrySet()) {
              result.put(entry.getKey().toString(),
                         toConnectData(valueSchema, entry.getValue(), toConnectContext));
            }
            converted = result;
          } else {
            // Arbitrary keys
            Collection<IndexedRecord> original = (Collection<IndexedRecord>) value;
            Map<Object, Object> result = new HashMap<>(original.size());
            for (IndexedRecord entry : original) {
              int avroKeyFieldIndex = entry.getSchema().getField(KEY_FIELD).pos();
              int avroValueFieldIndex = entry.getSchema().getField(VALUE_FIELD).pos();
              Object convertedKey = toConnectData(
                  keySchema, entry.get(avroKeyFieldIndex), toConnectContext);
              Object convertedValue = toConnectData(
                  valueSchema, entry.get(avroValueFieldIndex), toConnectContext);
              result.put(convertedKey, convertedValue);
            }
            converted = result;
          }
          break;
        }

        case STRUCT: {
          // Special case support for union types
          if (isUnionSchema(schema)) {
            Schema valueRecordSchema = null;
            if (value instanceof IndexedRecord) {
              IndexedRecord valueRecord = ((IndexedRecord) value);
              valueRecordSchema = toConnectSchemaWithCycles(
                  valueRecord.getSchema(), true, null, null, toConnectContext);
            }
            int index = 0;
            for (Field field : schema.fields()) {
              Schema fieldSchema = field.schema();
              if (isInstanceOfAvroSchemaTypeForSimpleSchema(fieldSchema, value, index)
                  || (valueRecordSchema != null && schemaEquals(valueRecordSchema, fieldSchema))) {
                converted = new Struct(schema).put(
                    unionMemberFieldName(fieldSchema, index),
                    toConnectData(fieldSchema, value, toConnectContext));
                break;
              }
              index++;
            }
            if (converted == null) {
              throw new DataException("Did not find matching union field for data");
            }
          } else if (value instanceof Map) {
            // Default values from Avro are returned as Map
            Map<CharSequence, Object> original = (Map<CharSequence, Object>) value;
            Struct result = new Struct(schema);
            for (Field field : schema.fields()) {
              String fieldName = scrubName(field.name());
              Object convertedFieldValue = toConnectData(field.schema(),
                  original.getOrDefault(fieldName, field.schema().defaultValue()),
                  toConnectContext);
              result.put(field, convertedFieldValue);
            }
            return result;
          } else {
            IndexedRecord original = (IndexedRecord) value;
            Struct result = new Struct(schema);
            for (Field field : schema.fields()) {
              String fieldName = scrubName(field.name());
              int avroFieldIndex = original.getSchema().getField(fieldName).pos();
              Object convertedFieldValue =
                  toConnectData(field.schema(), original.get(avroFieldIndex), toConnectContext);
              result.put(field, convertedFieldValue);
            }
            converted = result;
          }
          break;
        }

        default:
          throw new DataException("Unknown Connect schema type: " + schema.type());
      }

      if (schema.name() != null && doLogicalConversion) {
        LogicalTypeConverter logicalConverter = TO_CONNECT_LOGICAL_CONVERTERS.get(schema.name());
        if (logicalConverter != null) {
          converted = logicalConverter.convert(schema, converted);
        }
      }
      return converted;
    } catch (ClassCastException e) {
      String schemaType = schema != null ? schema.type().toString() : "null";
      throw new DataException("Invalid type for " + schemaType + ": " + value.getClass());
    }
  }

  protected boolean getForceOptionalDefault() {
    return false;
  }

  public Schema toConnectSchema(org.apache.avro.Schema schema) {
    return toConnectSchema(schema, null, new ToConnectContext());
  }


  private Schema toConnectSchema(org.apache.avro.Schema schema,
                                 Integer version,
                                 ToConnectContext toConnectContext) {

    // We perform caching only at this top level. While it might be helpful to cache some more of
    // the internal conversions, this is the safest place to add caching since some of the internal
    // conversions take extra flags (like forceOptional) which means the resulting schema might not
    // exactly match the Avro schema.
    AvroSchema schemaAndVersion = new AvroSchema(schema, version);
    Schema cachedSchema = toConnectSchemaCache.get(schemaAndVersion);
    if (cachedSchema != null) {
      if (schema.getType() == org.apache.avro.Schema.Type.RECORD) {
        // cycleReferences is only populated with record type schemas. We need to initialize it here
        // with the top-level record schema, as would happen if we did not hit the cache. This
        // schema has the version information set, thus it properly works with schemaEquals.
        toConnectContext.cycleReferences.put(schema, new CyclicSchemaWrapper(cachedSchema));
      }
      return cachedSchema;
    }

    Schema resultSchema = toConnectSchema(schema, getForceOptionalDefault(), null,
            null, version, toConnectContext);
    toConnectSchemaCache.put(schemaAndVersion, resultSchema);
    return resultSchema;
  }

  /**
   * @param schema           schema to convert
   * @param forceOptional    make the resulting schema optional, for converting Avro unions to a
   *                         record format and simple Avro unions of null + type to optional schemas
   * @param fieldDefaultVal  if non-null, override any connect-annotated default values with this
   *                         one; used when converting Avro record fields since they define default
   *                         values with the field spec, but Connect specifies them with the field's
   *                         schema
   * @param docDefaultVal    if non-null, override any connect-annotated documentation with this
   *                         one;
   *                         used when converting Avro record fields since they define doc values
   * @param toConnectContext context object that holds state while doing the conversion
   */
  private Schema toConnectSchema(org.apache.avro.Schema schema,
                                 boolean forceOptional,
                                 Object fieldDefaultVal,
                                 String docDefaultVal,
                                 ToConnectContext toConnectContext) {
    return toConnectSchema(
        schema, forceOptional, fieldDefaultVal, docDefaultVal, null, toConnectContext);

  }

  private Schema toConnectSchema(org.apache.avro.Schema schema,
                                 boolean forceOptional,
                                 Object fieldDefaultVal,
                                 String docDefaultVal,
                                 Integer version,
                                 ToConnectContext toConnectContext) {

    String type = schema.getProp(CONNECT_TYPE_PROP);
    String logicalType = schema.getProp(AVRO_LOGICAL_TYPE_PROP);

    final SchemaBuilder builder;

    switch (schema.getType()) {
      case BOOLEAN:
        builder = SchemaBuilder.bool();
        break;
      case BYTES:
      case FIXED:
        if (AVRO_LOGICAL_DECIMAL.equalsIgnoreCase(logicalType)) {
          Object scaleNode = schema.getObjectProp(AVRO_LOGICAL_DECIMAL_SCALE_PROP);
          // In Avro the scale is optional and should default to 0
          int scale = scaleNode instanceof Number ? ((Number) scaleNode).intValue() : 0;
          builder = Decimal.builder(scale);

          Object precisionNode = schema.getObjectProp(AVRO_LOGICAL_DECIMAL_PRECISION_PROP);
          if (null != precisionNode) {
            if (!(precisionNode instanceof Number)) {
              throw new DataException(AVRO_LOGICAL_DECIMAL_PRECISION_PROP
                  + " property must be a JSON Integer."
                  + " https://avro.apache.org/docs/1.9.1/spec.html#Decimal");
            }
            // Capture the precision as a parameter only if it is not the default
            int precision = ((Number) precisionNode).intValue();
            if (precision != CONNECT_AVRO_DECIMAL_PRECISION_DEFAULT) {
              builder.parameter(CONNECT_AVRO_DECIMAL_PRECISION_PROP, String.valueOf(precision));
            }
          }
        } else {
          builder = SchemaBuilder.bytes();
        }
        if (schema.getType() == org.apache.avro.Schema.Type.FIXED) {
          builder.parameter(CONNECT_AVRO_FIXED_SIZE_PROP, String.valueOf(schema.getFixedSize()));
        }
        break;
      case DOUBLE:
        builder = SchemaBuilder.float64();
        break;
      case FLOAT:
        builder = SchemaBuilder.float32();
        break;
      case INT:
        // INT is used for Connect's INT8, INT16, and INT32
        if (type == null && logicalType == null) {
          builder = SchemaBuilder.int32();
        } else if (logicalType != null) {
          if (AVRO_LOGICAL_DATE.equalsIgnoreCase(logicalType)) {
            builder = Date.builder();
          } else if (AVRO_LOGICAL_TIME_MILLIS.equalsIgnoreCase(logicalType)) {
            builder = Time.builder();
          } else {
            builder = SchemaBuilder.int32();
          }
        } else {
          Schema.Type connectType = NON_AVRO_TYPES_BY_TYPE_CODE.get(type);
          if (connectType == null) {
            throw new DataException("Connect type annotation for Avro int field is null");
          }
          builder = SchemaBuilder.type(connectType);
        }
        break;
      case LONG:
        if (AVRO_LOGICAL_TIMESTAMP_MILLIS.equalsIgnoreCase(logicalType)) {
          builder = Timestamp.builder();
        } else {
          builder = SchemaBuilder.int64();
        }
        break;
      case STRING:
        builder = SchemaBuilder.string();
        break;

      case ARRAY:
        org.apache.avro.Schema elemSchema = schema.getElementType();
        // Special case for custom encoding of non-string maps as list of key-value records
        if (isMapEntry(elemSchema)) {
          if (elemSchema.getFields().size() != 2
              || elemSchema.getField(KEY_FIELD) == null
              || elemSchema.getField(VALUE_FIELD) == null) {
            throw new DataException("Found map encoded as array of key-value pairs, but array "
                                    + "elements do not match the expected format.");
          }
          builder = SchemaBuilder.map(
              toConnectSchema(elemSchema.getField(KEY_FIELD).schema()),
              toConnectSchema(elemSchema.getField(VALUE_FIELD).schema())
          );
        } else {
          Schema arraySchema = toConnectSchemaWithCycles(
              schema.getElementType(), getForceOptionalDefault(),
                  null, null, toConnectContext);
          builder = SchemaBuilder.array(arraySchema);
        }
        break;

      case MAP:
        builder = SchemaBuilder.map(
            Schema.STRING_SCHEMA,
            toConnectSchemaWithCycles(schema.getValueType(), getForceOptionalDefault(),
                    null, null, toConnectContext)
        );
        break;

      case RECORD: {
        builder = SchemaBuilder.struct();
        toConnectContext.cycleReferences.put(schema, new CyclicSchemaWrapper(builder));
        if (!discardTypeDocDefault && connectMetaData && schema.getDoc() != null) {
          builder.parameter(AVRO_RECORD_DOC_PROP, schema.getDoc());
        }
        for (org.apache.avro.Schema.Field field : schema.getFields()) {
          if (!discardTypeDocDefault && connectMetaData && field.doc() != null) {
            builder.parameter(AVRO_FIELD_DOC_PREFIX_PROP + field.name(), field.doc());
          }
          Object defaultVal = null;
          try {
            defaultVal = field.defaultVal();
          } catch (Exception e) {
            log.warn("Ignoring invalid default for field {}", field, e);
          }
          Schema fieldSchema = toConnectSchema(field.schema(), getForceOptionalDefault(),
                  defaultVal, field.doc(), toConnectContext);
          builder.field(field.name(), fieldSchema);
        }
        break;
      }

      case ENUM:
        // enums are unwrapped to strings and the original enum is not preserved
        builder = SchemaBuilder.string();
        if (connectMetaData) {
          if (schema.getDoc() != null) {
            builder.parameter(discardTypeDocDefault
                ? CONNECT_ENUM_DOC_PROP
                : AVRO_ENUM_DOC_PREFIX_PROP + schema.getName(),
                schema.getDoc());
          }
          if (!discardTypeDocDefault && schema.getEnumDefault() != null) {
            builder.parameter(AVRO_ENUM_DEFAULT_PREFIX_PROP + schema.getName(),
                schema.getEnumDefault());
          }
        }
        String paramName = generalizedSumTypeSupport ? GENERALIZED_TYPE_ENUM : AVRO_TYPE_ENUM;
        builder.parameter(paramName, schema.getFullName());
        int symbolIndex = 0;
        for (String enumSymbol : schema.getEnumSymbols()) {
          if (generalizedSumTypeSupport) {
            builder.parameter(paramName + "." + enumSymbol, String.valueOf(symbolIndex));
          } else {
            builder.parameter(paramName + "." + enumSymbol, enumSymbol);
          }
          symbolIndex++;
        }
        break;

      case UNION: {
        if (schema.getTypes().size() == 1 && flattenSingletonUnion) {
          return toConnectSchemaWithCycles(schema.getTypes().get(0), getForceOptionalDefault(),
              null, docDefaultVal, toConnectContext);
        } else if (schema.getTypes().size() == 2) {
          if (schema.getTypes().contains(NULL_AVRO_SCHEMA)) {
            for (org.apache.avro.Schema memberSchema : schema.getTypes()) {
              if (!memberSchema.equals(NULL_AVRO_SCHEMA)) {
                return toConnectSchemaWithCycles(
                    memberSchema, true, null, docDefaultVal, toConnectContext);
              }
            }
          }
        }
        String unionName = generalizedSumTypeSupport
            ? GENERALIZED_TYPE_UNION_PREFIX + (unionIndex++)
            : AVRO_TYPE_UNION;
        builder = SchemaBuilder.struct().name(unionName);
        if (generalizedSumTypeSupport) {
          builder.parameter(GENERALIZED_TYPE_UNION, unionName);
        }
        Set<String> fieldNames = new HashSet<>();
        int fieldIndex = 0;
        for (org.apache.avro.Schema memberSchema : schema.getTypes()) {
          if (memberSchema.getType() == org.apache.avro.Schema.Type.NULL) {
            builder.optional();
          } else {
            String fieldName = unionMemberFieldName(memberSchema, fieldIndex);
            if (fieldNames.contains(fieldName)) {
              throw new DataException("Multiple union schemas map to the Connect union field name");
            }
            fieldNames.add(fieldName);
            builder.field(
                fieldName,
                toConnectSchemaWithCycles(memberSchema, true, null, null, toConnectContext)
            );
          }
          fieldIndex++;
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

    if (discardTypeDocDefault) {
      String docVal = docDefaultVal != null ? docDefaultVal :
          (schema.getDoc() != null ? schema.getDoc() : schema.getProp(CONNECT_DOC_PROP));
      if (docVal != null) {
        builder.doc(docVal);
      }
      if (connectMetaData && schema.getDoc() != null) {
        builder.parameter(CONNECT_RECORD_DOC_PROP, schema.getDoc());
      }

    } else {
      String docVal = schema.getProp(CONNECT_DOC_PROP);
      if (connectMetaData && docVal != null) {
        builder.doc(docVal);
      }
    }

    // Included Kafka Connect version takes priority, fall back to schema registry version
    int versionInt = -1;  // A valid version must be a positive integer (assumed throughout SR)
    Object versionNode = schema.getObjectProp(CONNECT_VERSION_PROP);
    if (versionNode != null) {
      if (!(versionNode instanceof Number)) {
        throw new DataException("Invalid Connect version found: " + versionNode.toString());
      }
      versionInt = ((Number) versionNode).intValue();
    } else if (version != null) {
      versionInt = version.intValue();
    }
    if (versionInt >= 0) {
      if (builder.version() != null) {
        if (versionInt != builder.version()) {
          throw new DataException("Mismatched versions: version already added to SchemaBuilder "
                                  + "("
                                  + builder.version()
                                  + ") differs from version in source schema ("
                                  + versionInt
                                  + ")");
        }
      } else {
        builder.version(versionInt);
      }
    }

    Object parameters = schema.getObjectProp(CONNECT_PARAMETERS_PROP);
    if (connectMetaData && parameters != null) {
      if (!(parameters instanceof Map)) {
        throw new DataException("Expected JSON object for schema parameters but found: "
            + parameters);
      }
      Iterator<Map.Entry<String, Object>> paramIt =
          ((Map<String, Object>) parameters).entrySet().iterator();
      while (paramIt.hasNext()) {
        Map.Entry<String, Object> field = paramIt.next();
        Object jsonValue = field.getValue();
        if (!(jsonValue instanceof String)) {
          throw new DataException("Expected schema parameter values to be strings but found: "
              + jsonValue);
        }
        builder.parameter(field.getKey(), (String) jsonValue);
      }
    }

    for (Map.Entry<String, Object> entry : schema.getObjectProps().entrySet()) {
      if (entry.getKey().startsWith(AVRO_PROP)) {
        builder.parameter(entry.getKey(), entry.getValue().toString());
      }
    }

    Object connectDefault = schema.getObjectProp(CONNECT_DEFAULT_VALUE_PROP);
    if (fieldDefaultVal == null) {
      fieldDefaultVal = JacksonUtils.toJsonNode(connectDefault);
    } else if (!discardTypeDocDefault && connectMetaData && connectDefault == null) {
      builder.parameter(AVRO_FIELD_DEFAULT_FLAG_PROP, "true");
    }
    if (fieldDefaultVal != null) {
      try {
        builder.defaultValue(
            defaultValueFromAvro(builder, schema, fieldDefaultVal, toConnectContext));
      } catch (DataException e) {
        log.warn("Ignoring invalid default for schema {}", schema.getName(), e);
      }
    }

    Object connectNameJson = schema.getObjectProp(CONNECT_NAME_PROP);
    String name = null;
    if (connectNameJson != null) {
      if (!(connectNameJson instanceof String)) {
        throw new DataException("Invalid schema name: " + connectNameJson.toString());
      }
      name = (String) connectNameJson;

    } else if (schema.getType() == org.apache.avro.Schema.Type.RECORD
        || schema.getType() == org.apache.avro.Schema.Type.ENUM
        || schema.getType() == org.apache.avro.Schema.Type.FIXED) {
      name = schema.getFullName();
    }
    if (name != null && !name.startsWith(DEFAULT_SCHEMA_FULL_NAME)) {
      if (builder.name() != null) {
        if (!name.equals(builder.name())) {
          throw new DataException("Mismatched names: name already added to SchemaBuilder ("
              + builder.name()
              + ") differs from name in source schema ("
              + name + ")");
        }
      } else {
        builder.name(name);
      }
    }

    if (forceOptional) {
      builder.optional();
    }

    if (!toConnectContext.detectedCycles.contains(schema)
        && toConnectContext.cycleReferences.containsKey(schema)) {
      toConnectContext.cycleReferences.remove(schema);
    }

    return builder.build();
  }

  private Schema toConnectSchemaWithCycles(org.apache.avro.Schema schema,
                                           boolean forceOptional,
                                           Object fieldDefaultVal,
                                           String docDefaultVal,
                                           ToConnectContext toConnectContext) {
    Schema resolvedSchema;
    if (toConnectContext.cycleReferences.containsKey(schema)) {
      toConnectContext.detectedCycles.add(schema);
      resolvedSchema = cyclicSchemaWrapper(toConnectContext.cycleReferences, schema, forceOptional);
    } else {
      resolvedSchema = toConnectSchema(
          schema, forceOptional, fieldDefaultVal, docDefaultVal, toConnectContext);
    }
    return resolvedSchema;
  }

  private CyclicSchemaWrapper cyclicSchemaWrapper(
      Map<org.apache.avro.Schema, CyclicSchemaWrapper> toConnectCycles,
      org.apache.avro.Schema memberSchema,
      boolean optional) {
    return new CyclicSchemaWrapper(toConnectCycles.get(memberSchema).schema(), optional);
  }

  private Object defaultValueFromAvro(Schema schema,
      org.apache.avro.Schema avroSchema,
      Object value,
      ToConnectContext toConnectContext) {
    Object result = defaultValueFromAvroWithoutLogical(schema, avroSchema, value, toConnectContext);
    // If the schema is a logical type, convert the primitive Avro default into the logical form
    return toConnectLogical(schema, result);
  }

  private Object defaultValueFromAvroWithoutLogical(Schema schema,
                                      org.apache.avro.Schema avroSchema,
                                      Object value,
                                      ToConnectContext toConnectContext) {
    if (value == null || value == JsonProperties.NULL_VALUE) {
      return null;
    }

    // The type will be JsonNode if this default was pulled from a Connect default field, or an
    // Object if it's the actual Avro-specified default. If it's a regular Java object, we can
    // use our existing conversion tools.
    if (!(value instanceof JsonNode)) {
      return toConnectData(schema, value, toConnectContext, false);
    }

    JsonNode jsonValue = (JsonNode) value;
    switch (avroSchema.getType()) {
      case INT:
        if (schema.type() == Schema.Type.INT8) {
          return (byte) jsonValue.intValue();
        } else if (schema.type() == Schema.Type.INT16) {
          return jsonValue.shortValue();
        } else if (schema.type() == Schema.Type.INT32) {
          return jsonValue.intValue();
        } else {
          break;
        }

      case LONG:
        return jsonValue.longValue();

      case FLOAT:
        return (float) jsonValue.doubleValue();
      case DOUBLE:
        return jsonValue.doubleValue();

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
          byte[] bytes;
          if (jsonValue.isTextual()) {
            // Avro's JSON form may be a quoted string, so decode the binary value
            String encoded = jsonValue.textValue();
            bytes = encoded.getBytes(StandardCharsets.ISO_8859_1);
          } else {
            bytes = jsonValue.binaryValue();
          }
          return bytes == null ? null : ByteBuffer.wrap(bytes);
        } catch (IOException e) {
          throw new DataException("Invalid binary data in default value", e);
        }

      case ARRAY: {
        if (!jsonValue.isArray()) {
          throw new DataException("Invalid JSON for array default value");
        }
        List<Object> result = new ArrayList<>(jsonValue.size());
        for (JsonNode elem : jsonValue) {
          Object converted = defaultValueFromAvro(
              schema.valueSchema(), avroSchema.getElementType(), elem, toConnectContext);
          result.add(converted);
        }
        return result;
      }

      case MAP: {
        if (!jsonValue.isObject()) {
          throw new DataException("Invalid JSON for map default value");
        }
        Map<String, Object> result = new HashMap<>(jsonValue.size());
        Iterator<Map.Entry<String, JsonNode>> fieldIt = jsonValue.fields();
        while (fieldIt.hasNext()) {
          Map.Entry<String, JsonNode> field = fieldIt.next();
          Object converted = defaultValueFromAvro(
              schema.valueSchema(), avroSchema.getValueType(), field.getValue(), toConnectContext);
          result.put(field.getKey(), converted);
        }
        return result;
      }

      case RECORD: {
        if (!jsonValue.isObject()) {
          throw new DataException("Invalid JSON for record default value");
        }

        Struct result = new Struct(schema);
        for (org.apache.avro.Schema.Field avroField : avroSchema.getFields()) {
          Field field = schema.field(avroField.name());
          JsonNode fieldJson = ((JsonNode) value).get(field.name());
          Object converted = defaultValueFromAvro(
              field.schema(), avroField.schema(), fieldJson, toConnectContext);
          result.put(avroField.name(), converted);
        }
        return result;
      }

      case UNION: {
        // Defaults must match first type
        org.apache.avro.Schema memberAvroSchema = avroSchema.getTypes().get(0);
        if (memberAvroSchema.getType() == org.apache.avro.Schema.Type.NULL) {
          return null;
        } else {
          return defaultValueFromAvro(
              schema.field(unionMemberFieldName(memberAvroSchema, 0)).schema(),
              memberAvroSchema,
              value,
              toConnectContext);
        }
      }
      default: {
        return null;
      }
    }
    return null;
  }


  private String unionMemberFieldName(org.apache.avro.Schema schema, int index) {
    if (generalizedSumTypeSupport) {
      return GENERALIZED_TYPE_UNION_FIELD_PREFIX + index;
    }
    if (schema.getType() == org.apache.avro.Schema.Type.RECORD
        || schema.getType() == org.apache.avro.Schema.Type.ENUM
        || schema.getType() == org.apache.avro.Schema.Type.FIXED) {
      if (enhancedSchemaSupport) {
        return schema.getFullName();
      } else {
        return splitName(schema.getName())[1];
      }
    }
    return schema.getType().getName();
  }

  private String unionMemberFieldName(Schema schema, int index) {
    if (generalizedSumTypeSupport) {
      return GENERALIZED_TYPE_UNION_FIELD_PREFIX + index;
    }
    if (schema.type() == Schema.Type.STRUCT || isEnumSchema(schema) || isFixedSchema(schema)) {
      if (enhancedSchemaSupport) {
        return scrubFullName(schema.name(), scrubInvalidNames);
      } else {
        return splitName(schema.name())[1];
      }
    }
    return CONNECT_TYPES_TO_AVRO_TYPES.get(schema.type()).getName();
  }

  private static boolean isUnionSchema(Schema schema) {
    return AVRO_TYPE_UNION.equals(schema.name()) || ConnectUnion.isUnion(schema);
  }

  private static boolean isEnumSchema(Schema schema) {
    return schema.type() == Schema.Type.STRING
           && schema.parameters() != null
           && (schema.parameters().containsKey(GENERALIZED_TYPE_ENUM)
               || schema.parameters().containsKey(AVRO_TYPE_ENUM));
  }

  private static boolean isFixedSchema(Schema schema) {
    return schema.type() == Schema.Type.BYTES
            && schema.name() != null
            && schema.parameters() != null
            && schema.parameters().containsKey(CONNECT_AVRO_FIXED_SIZE_PROP);
  }

  private boolean isInstanceOfAvroSchemaTypeForSimpleSchema(Schema fieldSchema,
                                                            Object value,
                                                            int index) {
    if (isEnumSchema(fieldSchema)) {
      String paramName = generalizedSumTypeSupport ? GENERALIZED_TYPE_ENUM : AVRO_TYPE_ENUM;
      String enumSchemaName = fieldSchema.parameters().get(paramName);
      if (value instanceof GenericData.EnumSymbol) {
        return ((GenericData.EnumSymbol) value).getSchema().getFullName().equals(enumSchemaName);
      } else {
        return value.getClass().getName().equals(enumSchemaName);
      }
    }
    List<Class> classes = SIMPLE_AVRO_SCHEMA_TYPES.get(fieldSchema.type());
    if (classes == null) {
      return false;
    }
    for (Class type : classes) {
      if (type.isInstance(value)) {
        if (isFixedSchema(fieldSchema)) {
          if (fixedValueSizeMatch(fieldSchema, value,
              Integer.parseInt(fieldSchema.parameters().get(CONNECT_AVRO_FIXED_SIZE_PROP)),
              index)) {
            return true;
          }
        } else {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns true if the fixed value size of the value matches the expected size
   */
  private boolean fixedValueSizeMatch(Schema fieldSchema,
                                      Object value,
                                      int size,
                                      int index) {
    if (value instanceof byte[]) {
      return ((byte[]) value).length == size;
    } else if (value instanceof ByteBuffer) {
      return ((ByteBuffer)value).remaining() == size;
    } else if (value instanceof GenericFixed) {
      return unionMemberFieldName(((GenericFixed) value).getSchema(), index)
              .equals(unionMemberFieldName(fieldSchema, index));
    } else {
      throw new DataException("Invalid class for fixed, expecting GenericFixed, byte[]"
              + " or ByteBuffer but found " + value.getClass());
    }
  }

  /**
   * Split a full dotted-syntax name into a namespace and a single-component name.
   */
  private String[] splitName(String fullName) {
    return splitName(fullName, scrubInvalidNames);
  }

  private static String[] splitName(String fullName, boolean scrubInvalidNames) {
    String[] result = new String[2];
    if (fullName == null || fullName.isEmpty()) {
      result[0] = null;
      result[1] = fullName;
      return result;
    }
    String[] parts = fullName.split("\\.");
    for (int i = 0; i < parts.length; i++) {
      parts[i] = scrubName(parts[i], scrubInvalidNames);
    }
    if (parts.length <= 1) {
      result[0] = null;
      result[1] = parts[0];
    } else {
      result[0] = String.join(".", Arrays.copyOfRange(parts, 0, parts.length - 1));
      result[1] = parts[parts.length - 1];
    }
    return result;
  }

  private interface LogicalTypeConverter {

    Object convert(Schema schema, Object value);
  }

  public static Schema nonOptional(Schema schema) {
    return new ConnectSchema(schema.type(), false, schema.defaultValue(), schema.name(),
                             schema.version(), schema.doc(),
                             schema.parameters(),
                             fields(schema),
                             keySchema(schema),
                             valueSchema(schema));
  }

  public static List<Field> fields(Schema schema) {
    Schema.Type type = schema.type();
    if (Schema.Type.STRUCT.equals(type)) {
      return schema.fields();
    } else {
      return null;
    }
  }

  public static Schema keySchema(Schema schema) {
    Schema.Type type = schema.type();
    if (Schema.Type.MAP.equals(type)) {
      return schema.keySchema();
    } else {
      return null;
    }
  }

  public static Schema valueSchema(Schema schema) {
    Schema.Type type = schema.type();
    if (Schema.Type.MAP.equals(type) || Schema.Type.ARRAY.equals(type)) {
      return schema.valueSchema();
    } else {
      return null;
    }
  }

  private static boolean fieldListEquals(List<Field> one, List<Field> two,
                                         Map<Pair<Schema, Schema>, Boolean> cache) {
    if (one == two) {
      return true;
    } else if (one == null || two == null) {
      return false;
    } else {
      ListIterator<Field> itOne = one.listIterator();
      ListIterator<Field> itTwo = two.listIterator();
      while (itOne.hasNext() && itTwo.hasNext()) {
        if (!fieldEquals(itOne.next(), itTwo.next(), cache)) {
          return false;
        }
      }
      return itOne.hasNext() == itTwo.hasNext();
    }
  }

  private static boolean fieldEquals(
      Field one, Field two, Map<Pair<Schema, Schema>, Boolean> cache) {
    if (one == two) {
      return true;
    } else if (one == null || two == null) {
      return false;
    } else {
      return one.getClass() == two.getClass()
              && Objects.equals(one.index(), two.index())
              && Objects.equals(one.name(), two.name())
              && schemaEquals(one.schema(), two.schema(), cache);
    }
  }

  static class Pair<K, V> {
    private K key;
    private V value;

    public Pair(K key, V value) {
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
      Pair<?, ?> pair = (Pair<?, ?>) o;
      return Objects.equals(key, pair.key)
          && Objects.equals(value, pair.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, value);
    }

    @Override
    public String toString() {
      return "Pair{"
          + "key=" + key
          + ", value=" + value
          + '}';
    }
  }

  private static boolean schemaEquals(Schema src, Schema that) {
    return schemaEquals(src, that, new HashMap<>());
  }

  private static boolean schemaEquals(
      Schema src, Schema that, Map<Pair<Schema, Schema>, Boolean> cache) {
    if (src == that) {
      return true;
    } else if (src == null || that == null) {
      return false;
    }

    // Add a temporary value to the cache to avoid cycles. As long as we recurse only at the end of
    // the method, we can safely default to true here. The cache is updated at the end of the method
    // with the actual comparison result.
    Pair<Schema, Schema> sp = new Pair<>(src, that);
    Boolean cacheHit = cache.putIfAbsent(sp, true);
    if (cacheHit != null) {
      return cacheHit;
    }

    boolean equals = Objects.equals(src.isOptional(), that.isOptional())
        && Objects.equals(src.version(), that.version())
        && Objects.equals(src.name(), that.name())
        && Objects.equals(src.doc(), that.doc())
        && Objects.equals(src.type(), that.type())
        && Objects.deepEquals(src.defaultValue(), that.defaultValue())
        && Objects.equals(src.parameters(), that.parameters());

    switch (src.type()) {
      case STRUCT:
        equals = equals && fieldListEquals(src.fields(), that.fields(), cache);
        break;
      case ARRAY:
        equals = equals && schemaEquals(src.valueSchema(), that.valueSchema(), cache);
        break;
      case MAP:
        equals = equals
                && schemaEquals(src.valueSchema(), that.valueSchema(), cache)
                && schemaEquals(src.keySchema(), that.keySchema(), cache);
        break;
      default:
        break;
    }
    cache.put(sp, equals);
    return equals;
  }

  private static class CyclicSchemaWrapper implements Schema {

    private final Schema schema;
    private final boolean optional;

    public CyclicSchemaWrapper(Schema schema) {
      this(schema, schema.isOptional());
    }

    public CyclicSchemaWrapper(Schema schema, boolean optional) {
      this.schema = schema;
      this.optional = optional;
    }

    @Override
    public Type type() {
      return schema.type();
    }

    @Override
    public boolean isOptional() {
      return optional;
    }

    @Override
    public Object defaultValue() {
      return schema.defaultValue();
    }

    @Override
    public String name() {
      return schema.name();
    }

    @Override
    public Integer version() {
      return schema.version();
    }

    @Override
    public String doc() {
      return schema.doc();
    }

    @Override
    public Map<String, String> parameters() {
      return schema.parameters();
    }

    @Override
    public Schema keySchema() {
      return schema.keySchema();
    }

    @Override
    public Schema valueSchema() {
      return schema.valueSchema();
    }

    @Override
    public List<Field> fields() {
      return schema.fields();
    }

    @Override
    public Field field(String s) {
      return schema.field(s);
    }

    @Override
    public Schema schema() {
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

      CyclicSchemaWrapper other = (CyclicSchemaWrapper) o;
      return Objects.equals(optional, other.optional) && Objects.equals(schema, other.schema);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(optional) + Objects.hashCode(schema);
    }
  }

  /**
   * Class that holds the context for performing {@code toConnectSchema}
   */
  private static class ToConnectContext {
    private final Map<org.apache.avro.Schema, CyclicSchemaWrapper> cycleReferences;
    private final Set<org.apache.avro.Schema> detectedCycles;

    /**
     * cycleReferences - map that holds connect Schema references to resolve cycles
     * detectedCycles - avro schemas that have been detected to have cycles
     */
    private ToConnectContext() {
      this.cycleReferences = new IdentityHashMap<>();
      this.detectedCycles = new HashSet<>();
    }
  }

  /**
   * Class that holds the context for performing {@code fromConnectSchema}
   */
  private static class FromConnectContext {
    //SchemaMap is used to resolve references that need to mapped as types
    private final Map<Schema, org.apache.avro.Schema> schemaMap;
    //schema name to Schema reference to resolve cycles
    private final Map<String, org.apache.avro.Schema> cycleReferences;
    private int defaultSchemaNameIndex = 0;

    private FromConnectContext(Map<Schema, org.apache.avro.Schema> schemaMap) {
      this.schemaMap = schemaMap;
      this.cycleReferences = new IdentityHashMap<>();
    }

    public int incrementAndGetNameIndex() {
      return ++defaultSchemaNameIndex;
    }
  }
}
