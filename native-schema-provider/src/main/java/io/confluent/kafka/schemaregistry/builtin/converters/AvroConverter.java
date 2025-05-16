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

package io.confluent.kafka.schemaregistry.builtin.converters;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.builtin.NativeSchema;
import io.confluent.kafka.schemaregistry.builtin.Schema;
import io.confluent.kafka.schemaregistry.builtin.Schema.Field;
import io.confluent.kafka.schemaregistry.builtin.SchemaBuilder;
import io.confluent.kafka.schemaregistry.builtin.SchemaBuilder.BaseTypeBuilder;
import io.confluent.kafka.schemaregistry.builtin.SchemaBuilder.FieldAssembler;
import io.confluent.kafka.schemaregistry.builtin.SchemaBuilder.PropBuilder;
import io.confluent.kafka.schemaregistry.builtin.SchemaBuilder.UnionAccumulator;
import io.confluent.kafka.schemaregistry.builtin.SchemaRuntimeException;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for converting between our runtime data format and Avro, and (de)serializing that data.
 */
public class AvroConverter {

  private static final Logger log = LoggerFactory.getLogger(AvroConverter.class);

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

  public static final String CFLT_TYPE_PROP = "connect.type";

  public static final String CFLT_TYPE_INT8 = "int8";
  public static final String CFLT_TYPE_INT16 = "int16";

  public static final String AVRO_TYPE_UNION = NAMESPACE + ".Union";
  public static final String AVRO_TYPE_ENUM = NAMESPACE + ".Enum";

  public static final String AVRO_TYPE_ANYTHING = NAMESPACE + ".Anything";

  private static final Map<String, Schema.Type> NON_AVRO_TYPES_BY_TYPE_CODE = new HashMap<>();

  static {
    NON_AVRO_TYPES_BY_TYPE_CODE.put(CFLT_TYPE_INT8, Schema.Type.INT8);
    NON_AVRO_TYPES_BY_TYPE_CODE.put(CFLT_TYPE_INT16, Schema.Type.INT16);
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

  private static final org.apache.avro.Schema
      NULL_AVRO_SCHEMA =
      org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL);

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

  private int unionIndex = 0;

  private Map<NativeSchema, AvroSchema> fromNativeSchemaCache;
  private Map<AvroSchema, NativeSchema> toNativeSchemaCache;
  private boolean connectMetaData;
  private boolean generalizedSumTypeSupport;
  private boolean ignoreDefaultForNullables;
  private boolean enhancedSchemaSupport;
  private boolean scrubInvalidNames;
  private boolean discardTypeDocDefault;
  private boolean allowOptionalMapKey;
  private boolean flattenSingletonUnions;

  public AvroConverter(int cacheSize) {
    fromNativeSchemaCache = new BoundedConcurrentHashMap<>(cacheSize);
    toNativeSchemaCache = new BoundedConcurrentHashMap<>(cacheSize);
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
    return new EnumSymbol(enumSchema, (String) value);
  }

  public AvroSchema fromNativeSchema(NativeSchema schema) {
    if (schema == null) {
      return null;
    }

    AvroSchema cached = fromNativeSchemaCache.get(schema);
    if (cached != null) {
      return cached;
    }

    FromNativeContext fromNativeContext = new FromNativeContext();
    org.apache.avro.Schema finalSchema = fromNativeSchema(schema.rawSchema(), fromNativeContext);
    AvroSchema resultSchema = new AvroSchema(finalSchema);
    fromNativeSchemaCache.put(schema, resultSchema);
    return resultSchema;
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
  public org.apache.avro.Schema fromNativeSchema(Schema schema,
                                                 FromNativeContext fromNativeContext) {
    if (schema == null) {
      return null;
    }

    // Extra type annotation information for otherwise lossy conversions
    String cfltType = null;

    final org.apache.avro.Schema baseSchema;
    int size = -1;
    switch (schema.getType()) {
      case INT8:
        cfltType = CFLT_TYPE_INT8;
        baseSchema = org.apache.avro.SchemaBuilder.builder().intType();
        break;
      case INT16:
        cfltType = CFLT_TYPE_INT16;
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
      case CHAR:
        size = schema.getFixedSize();
        baseSchema = org.apache.avro.SchemaBuilder.builder().stringType();
        break;
      case STRING:
        baseSchema = org.apache.avro.SchemaBuilder.builder().stringType();
        break;
      case BINARY:
        size = schema.getFixedSize();
        baseSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
        break;
      case BYTES:
        size = schema.getFixedSize();
        baseSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
        break;
      case ARRAY:
        baseSchema = org.apache.avro.SchemaBuilder.builder().array()
            .items(fromNativeSchema(schema.getValueType(), fromNativeContext));
        break;
      case MAP:
        // Avro only supports string keys, so we match the representation when possible, but
        // otherwise fall back on a record representation
        if (schema.getKeyType().getType() == Schema.Type.STRING) {
          baseSchema = org.apache.avro.SchemaBuilder.builder().map().values(
              fromNativeSchema(schema.getValueType(), fromNativeContext));
        } else {
          // Special record name indicates format
          List<org.apache.avro.Schema.Field> fields = new ArrayList<>();
          final org.apache.avro.Schema mapSchema = org.apache.avro.Schema.createRecord(
                MAP_ENTRY_TYPE_NAME,
                null,
                NAMESPACE,
                false
            );
          addAvroRecordField(
              fields,
              KEY_FIELD,
              schema.getKeyType(),
              null,
              null,
              fromNativeContext);
          addAvroRecordField(
              fields,
              VALUE_FIELD,
              schema.getValueType(),
              null,
              null,
              fromNativeContext);
          mapSchema.setFields(fields);
          baseSchema = org.apache.avro.Schema.createArray(mapSchema);
        }
        break;
      case STRUCT:
        String namespace = schema.getNamespace();
        String name = schema.getName();
        String doc = schema.getDoc();
        baseSchema = org.apache.avro.Schema.createRecord(name, doc, namespace, false);
        List<org.apache.avro.Schema.Field> fields = new ArrayList<>();
        for (Field field : schema.getFields()) {
          String fieldName = field.name();
          String fieldDoc = field.doc();
          addAvroRecordField(fields, fieldName, field.schema(),
              field.defaultVal(), fieldDoc, fromNativeContext);
        }
        baseSchema.setFields(fields);
        break;
      case UNION:
        List<org.apache.avro.Schema> unionSchemas = new ArrayList<>();
        for (Schema subschema : schema.getTypes()) {
          unionSchemas.add(fromNativeSchema(subschema, fromNativeContext));
        }
        baseSchema = org.apache.avro.Schema.createUnion(unionSchemas);
        break;
      default:
        throw new SchemaRuntimeException("Unknown schema type: " + schema.getType());
    }

    org.apache.avro.Schema finalSchema = baseSchema;
    /*
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
        if (cfltType != null) {
          baseSchema.addProp(CFLT_TYPE_PROP, cfltType);
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
      fromNativeContext.schemaMap.put(schema, finalSchema);
    }
     */
    return finalSchema;
  }

  private void addAvroRecordField(
      List<org.apache.avro.Schema.Field> fields,
      String fieldName, Schema fieldSchema,
      Object fieldDefault, String fieldDoc,
      FromNativeContext fromNativeContext) {

    org.apache.avro.Schema.Field field;
    org.apache.avro.Schema schema = fromNativeSchema(fieldSchema, fromNativeContext);
    field = new org.apache.avro.Schema.Field(
        fieldName,
        schema,
        fieldDoc,
        fieldDefault);
    fields.add(field);
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

  protected boolean getForceOptionalDefault() {
    return false;
  }

  private NativeSchema toNativeSchema(AvroSchema schema, ToNativeContext toNativeContext) {

    NativeSchema cachedSchema = toNativeSchemaCache.get(schema);
    if (cachedSchema != null) {
      return cachedSchema;
    }

    Schema nativeSchema = toNativeSchema(schema.rawSchema(), toNativeContext);
    NativeSchema resultSchema = new NativeSchema(nativeSchema);
    toNativeSchemaCache.put(schema, resultSchema);
    return resultSchema;
  }

  private Schema toNativeSchema(
      org.apache.avro.Schema schema, ToNativeContext toNativeContext) {

    String type = schema.getProp(CFLT_TYPE_PROP);
    String logicalType = schema.getProp(AVRO_LOGICAL_TYPE_PROP);

    final PropBuilder<?> builder;

    switch (schema.getType()) {
      case BOOLEAN:
        return SchemaBuilder.builder().booleanBuilder().endBoolean();
      case BYTES:
        return SchemaBuilder.builder().bytesBuilder().endBytes();
      case FIXED:
        return SchemaBuilder.builder().fixedBinaryBuilder().size(schema.getFixedSize());
      case DOUBLE:
        return SchemaBuilder.builder().doubleBuilder().endDouble();
      case FLOAT:
        return SchemaBuilder.builder().floatBuilder().endFloat();
      case INT:
        // INT is used for Connect's INT8, INT16, and INT32
        // TODO RAY
        return SchemaBuilder.builder().intBuilder().endInt();
      case LONG:
        return SchemaBuilder.builder().longBuilder().endLong();
      case STRING:
        return SchemaBuilder.builder().stringBuilder().endString();
      case ARRAY:
        org.apache.avro.Schema elemSchema = schema.getElementType();
        // Special case for custom encoding of non-string maps as list of key-value records
        if (isMapEntry(elemSchema)) {
          if (elemSchema.getFields().size() != 2
              || elemSchema.getField(KEY_FIELD) == null
              || elemSchema.getField(VALUE_FIELD) == null) {
            throw new SchemaRuntimeException(
                "Found map encoded as array of key-value pairs, but array "
                    + "elements do not match the expected format.");
          }
          return SchemaBuilder.map()
              .keys(toNativeSchema(elemSchema.getField(KEY_FIELD).schema(), toNativeContext))
              .values(toNativeSchema(elemSchema.getField(VALUE_FIELD).schema(), toNativeContext));
        } else {
          Schema arraySchema = toNativeSchema(schema.getElementType(), toNativeContext);
          return SchemaBuilder.array().items(arraySchema);
        }

      case MAP:
        org.apache.avro.Schema valueSchema = schema.getValueType();
        return SchemaBuilder.map()
            .keys(SchemaBuilder.builder().stringType())
            .values(toNativeSchema(valueSchema, toNativeContext));

      case RECORD: {
        String name = schema.getName();
        String namespace = schema.getNamespace();
        FieldAssembler<Schema> fields =
            SchemaBuilder.builder().struct(name).namespace(namespace).fields();
        for (org.apache.avro.Schema.Field field : schema.getFields()) {
          Schema fieldSchema = toNativeSchema(field.schema(), toNativeContext);
          // TODO default
          fields = fields.name(field.name()).type(fieldSchema).noDefault();
        }
        return fields.endStruct();
      }

      case ENUM:
        String enumName = schema.getName();
        String[] symbols = schema.getEnumSymbols().toArray(new String[0]);
        return SchemaBuilder.enumeration(enumName).symbols(symbols);

      case UNION: {
        BaseTypeBuilder<UnionAccumulator<Schema>> union = SchemaBuilder.unionOf();

        UnionAccumulator<Schema> acc = null;
        for (org.apache.avro.Schema memberSchema : schema.getTypes()) {
          if (acc != null) {
            acc = acc.and().type(toNativeSchema(memberSchema, toNativeContext));
          } else {
            acc = union.type(toNativeSchema(memberSchema, toNativeContext));
          }
        }
        return acc.endUnion();
      }

      case NULL:
        return SchemaBuilder.builder().nullType();

      default:
        throw new SchemaRuntimeException("Couldn't translate unsupported schema type "
                                + schema.getType().getName() + ".");
    }

    /*
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
            defaultValueFromAvro(builder, schema, fieldDefaultVal, toNativeContext));
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

    if (!toNativeContext.detectedCycles.contains(schema)
        && toNativeContext.cycleReferences.containsKey(schema)) {
      toNativeContext.cycleReferences.remove(schema);
    }

    return builder.build();

     */
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

  /**
   * Class that holds the context for performing {@code toNativeSchema}
   */
  private static class ToNativeContext {
    private final Set<org.apache.avro.Schema> detectedCycles;

    /**
     * cycleReferences - map that holds connect Schema references to resolve cycles
     * detectedCycles - avro schemas that have been detected to have cycles
     */
    private ToNativeContext() {
      this.detectedCycles = new HashSet<>();
    }
  }

  /**
   * Class that holds the context for performing {@code fromNativeSchema}
   */
  private static class FromNativeContext {
    //SchemaMap is used to resolve references that need to mapped as types
    private Map<Schema, AvroSchema> schemaMap;
    //schema name to Schema reference to resolve cycles
    private int defaultSchemaNameIndex = 0;

    private FromNativeContext() {
    }

    public int incrementAndGetNameIndex() {
      return ++defaultSchemaNameIndex;
    }
  }
}
