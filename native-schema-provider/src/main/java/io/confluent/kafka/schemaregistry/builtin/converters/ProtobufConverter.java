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
 *
 */

package io.confluent.kafka.schemaregistry.builtin.converters;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.Edition;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import io.confluent.kafka.schemaregistry.builtin.NativeSchema;
import io.confluent.kafka.schemaregistry.builtin.Schema;
import io.confluent.kafka.schemaregistry.builtin.Schema.Field;
import io.confluent.kafka.schemaregistry.builtin.Schema.Type;
import io.confluent.kafka.schemaregistry.builtin.SchemaBuilder;
import io.confluent.kafka.schemaregistry.builtin.SchemaBuilder.BaseTypeBuilder;
import io.confluent.kafka.schemaregistry.builtin.SchemaBuilder.FieldAssembler;
import io.confluent.kafka.schemaregistry.builtin.SchemaBuilder.FieldBuilder;
import io.confluent.kafka.schemaregistry.builtin.SchemaBuilder.FieldTypeBuilder;
import io.confluent.kafka.schemaregistry.builtin.SchemaBuilder.StructBuilder;
import io.confluent.kafka.schemaregistry.builtin.SchemaRuntimeException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.ProtobufMeta;
import io.confluent.kafka.schemaregistry.protobuf.diff.Context;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.DynamicSchema;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.EnumDefinition;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.FieldDefinition;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.MessageDefinition;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;
import io.confluent.protobuf.MetaProto;
import io.confluent.protobuf.MetaProto.Meta;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;


public class ProtobufConverter {

  public static final String NAMESPACE = "io.confluent.connect.protobuf";

  public static final String DEFAULT_SCHEMA_NAME = "ConnectDefault";
  public static final String MAP_ENTRY_SUFFIX = ProtobufSchema.MAP_ENTRY_SUFFIX;  // Suffix used
  // by protoc
  public static final String KEY_FIELD = ProtobufSchema.KEY_FIELD;
  public static final String VALUE_FIELD = ProtobufSchema.VALUE_FIELD;

  public static final String PROTOBUF_TYPE_ENUM = NAMESPACE + ".Enum";
  public static final String PROTOBUF_TYPE_ENUM_PREFIX = PROTOBUF_TYPE_ENUM + ".";
  public static final String PROTOBUF_TYPE_UNION = NAMESPACE + ".Union";
  public static final String PROTOBUF_TYPE_UNION_PREFIX = PROTOBUF_TYPE_UNION + ".";
  public static final String PROTOBUF_TYPE_TAG = NAMESPACE + ".Tag";
  public static final String PROTOBUF_TYPE_PROP = NAMESPACE + ".Type";

  public static final String PROTOBUF_PRECISION_PROP = "precision";
  public static final String PROTOBUF_SCALE_PROP = "scale";
  public static final String PROTOBUF_DECIMAL_LOCATION = "confluent/type/decimal.proto";
  public static final String PROTOBUF_DECIMAL_TYPE = "confluent.type.Decimal";
  public static final String PROTOBUF_DATE_LOCATION = "google/type/date.proto";
  public static final String PROTOBUF_DATE_TYPE = "google.type.Date";
  public static final String PROTOBUF_TIME_LOCATION = "google/type/timeofday.proto";
  public static final String PROTOBUF_TIME_TYPE = "google.type.TimeOfDay";
  public static final String PROTOBUF_TIMESTAMP_LOCATION = "google/protobuf/timestamp.proto";
  public static final String PROTOBUF_TIMESTAMP_TYPE = "google.protobuf.Timestamp";

  public static final String PROTOBUF_WRAPPER_LOCATION = "google/protobuf/wrappers.proto";
  public static final String PROTOBUF_DOUBLE_WRAPPER_TYPE = "google.protobuf.DoubleValue";
  public static final String PROTOBUF_FLOAT_WRAPPER_TYPE = "google.protobuf.FloatValue";
  public static final String PROTOBUF_INT64_WRAPPER_TYPE = "google.protobuf.Int64Value";
  public static final String PROTOBUF_UINT64_WRAPPER_TYPE = "google.protobuf.UInt64Value";
  public static final String PROTOBUF_INT32_WRAPPER_TYPE = "google.protobuf.Int32Value";
  public static final String PROTOBUF_UINT32_WRAPPER_TYPE = "google.protobuf.UInt32Value";
  public static final String PROTOBUF_BOOL_WRAPPER_TYPE = "google.protobuf.BoolValue";
  public static final String PROTOBUF_STRING_WRAPPER_TYPE = "google.protobuf.StringValue";
  public static final String PROTOBUF_BYTES_WRAPPER_TYPE = "google.protobuf.BytesValue";

  public static final String CONNECT_PRECISION_PROP = "connect.decimal.precision";
  public static final String CONNECT_SCALE_PROP = "connect.scale";
  public static final String CONNECT_TYPE_PROP = "connect.type";
  public static final String CONNECT_TYPE_INT8 = "int8";
  public static final String CONNECT_TYPE_INT16 = "int16";

  public static final String GENERALIZED_TYPE_UNION = "Union";
  public static final String GENERALIZED_TYPE_ENUM = "Enum";

  private static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000;
  private static final int MILLIS_PER_NANO = 1_000_000;
  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

  private final Map<NativeSchema, ProtobufSchema> fromConnectSchemaCache;
  private final Map<Pair<String, ProtobufSchema>, NativeSchema> toConnectSchemaCache;
  private boolean generalizedSumTypeSupport;
  private boolean ignoreDefaultForNullables;
  private boolean enhancedSchemaSupport;
  private boolean scrubInvalidNames;
  private boolean useIntForEnums;
  private boolean useOptionalForNullables;
  private boolean supportOptionalForProto2;
  private boolean useWrapperForNullables;
  private boolean useWrapperForRawPrimitives;
  private boolean generateStructForNulls;
  private boolean generateIndexForUnions;
  private boolean flattenUnions;

  public ProtobufConverter(int cacheSize) {
    fromConnectSchemaCache = new BoundedConcurrentHashMap<>(cacheSize);
    toConnectSchemaCache = new BoundedConcurrentHashMap<>(cacheSize);
  }

  private boolean isWrapper(ProtobufSchema protobufSchema) {
    String name = protobufSchema.name();
    switch (name) {
      case PROTOBUF_DOUBLE_WRAPPER_TYPE:
      case PROTOBUF_FLOAT_WRAPPER_TYPE:
      case PROTOBUF_INT64_WRAPPER_TYPE:
      case PROTOBUF_UINT64_WRAPPER_TYPE:
      case PROTOBUF_INT32_WRAPPER_TYPE:
      case PROTOBUF_UINT32_WRAPPER_TYPE:
      case PROTOBUF_BOOL_WRAPPER_TYPE:
      case PROTOBUF_STRING_WRAPPER_TYPE:
      case PROTOBUF_BYTES_WRAPPER_TYPE:
        return true;
      default:
        return false;
    }
  }

  private Object getFieldType(Object ctx, String name) {
    FieldDescriptor field = ((Descriptor) ctx).findFieldByName(name);
    if (field == null) {
      // Could not find a field with this name, which is the case with oneOfs.
      // In this case we just return the current Descriptor context,
      // since finding oneOf field names can be achieved with the enclosing Descriptor.
      return ctx;
    }
    return getFieldType(field);
  }

  private Object getFieldType(FieldDescriptor field) {
    switch (field.getJavaType()) {
      case MESSAGE:
        return field.getMessageType();
      case ENUM:
        return field.getEnumType();
      default:
        return field.getJavaType();
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

  public ProtobufSchema fromNativeSchema(NativeSchema schema) {
    if (schema == null) {
      return null;
    }
    ProtobufSchema cachedSchema = fromConnectSchemaCache.get(schema);
    if (cachedSchema != null) {
      return cachedSchema;
    }
    FromConnectContext ctx = new FromConnectContext();
    String fullName = getNameOrDefault(ctx, schema.name());
    String[] split = splitName(fullName);
    String namespace = split[0];
    String name = split[1];
    Descriptor descriptor = descriptorFromNativeSchema(ctx, namespace, name, schema);
    ProtobufSchema resultSchema = new ProtobufSchema(descriptor);
    fromConnectSchemaCache.put(schema, resultSchema);
    return resultSchema;
  }

  private Descriptor descriptorFromNativeSchema(
      FromConnectContext ctx, String namespace, String name, NativeSchema rootElem) {
    Type type = rootElem.rawSchema().getType();
    switch (type) {
      case INT8:
        return typeToDescriptor(PROTOBUF_INT32_WRAPPER_TYPE);
      case INT16:
        return typeToDescriptor(PROTOBUF_INT32_WRAPPER_TYPE);
      case INT32:
        return typeToDescriptor(PROTOBUF_INT32_WRAPPER_TYPE);
      case INT64:
        return typeToDescriptor(PROTOBUF_INT64_WRAPPER_TYPE);
      case FLOAT32:
        return typeToDescriptor(PROTOBUF_FLOAT_WRAPPER_TYPE);
      case FLOAT64:
        return typeToDescriptor(PROTOBUF_DOUBLE_WRAPPER_TYPE);
      case BOOLEAN:
        return typeToDescriptor(PROTOBUF_BOOL_WRAPPER_TYPE);
      case STRING:
        return typeToDescriptor(PROTOBUF_STRING_WRAPPER_TYPE);
      case BYTES:
        return typeToDescriptor(PROTOBUF_BYTES_WRAPPER_TYPE);
      case STRUCT:
        DynamicSchema dynamicSchema = rawSchemaFromNativeSchema(ctx, namespace, name, rootElem.rawSchema());
        return dynamicSchema.getMessageDescriptor(name);
      case ARRAY:
      case MAP:
      default:
        throw new IllegalArgumentException("Unsupported root schema of type " + type);
    }
  }

  /*
   * DynamicSchema is used as a temporary helper class and should not be exposed in the API.
   */
  private DynamicSchema rawSchemaFromNativeSchema(
      FromConnectContext ctx, String namespace, String name, Schema rootElem) {
    if (rootElem.getType() != Schema.Type.STRUCT) {
      throw new IllegalArgumentException("Unsupported root schema of type " + rootElem.getType());
    }
    try {
      DynamicSchema.Builder schema = DynamicSchema.newBuilder();
      schema.setSyntax(ProtobufSchema.PROTO3);
      if (namespace != null) {
        schema.setPackage(namespace);
      }
      schema.addMessageDefinition(messageDefinitionFromConnectSchema(ctx, schema, name, rootElem));
      return schema.build();
    } catch (Descriptors.DescriptorValidationException e) {
      throw new IllegalStateException(e);
    }
  }

  private MessageDefinition messageDefinitionFromConnectSchema(
      FromConnectContext ctx, DynamicSchema.Builder schema, String name, Schema messageElem
  ) {
    MessageDefinition.Builder message = MessageDefinition.newBuilder(name);
    int index = 1;
    for (Field field : messageElem.getFields()) {
      Schema fieldSchema = field.schema();
      String fieldTag = fieldSchema.getProp(PROTOBUF_TYPE_TAG);
      int tag = fieldTag != null ? Integer.parseInt(fieldTag) : index++;
      FieldDefinition.Builder fieldDef = fieldDefinitionFromConnectSchema(
          ctx,
          schema,
          message,
          fieldSchema,
          scrubName(field.name()),
          field.defaultVal(),
          tag
      );
      if (fieldDef != null) {
        boolean isProto3Optional = "optional".equals(getLabel(fieldSchema));
        if (isProto3Optional) {
          // Add a synthentic oneof
          MessageDefinition.OneofBuilder oneofBuilder = message.addOneof("_" + fieldDef.getName());
          fieldDef.setProto3Optional(true);
          fieldDef.setOneofIndex(oneofBuilder.getIdx());
          oneofBuilder.addField(fieldDef.build());
        } else {
          message.addField(fieldDef.build());
        }
      }
    }
    return message.build();
  }

  private void oneofDefinitionFromConnectSchema(
      FromConnectContext ctx,
      DynamicSchema.Builder schema,
      MessageDefinition.Builder message,
      Schema unionElem,
      String unionName
  ) {
    /*
    MessageDefinition.OneofBuilder oneof = message.addOneof(unionName);
    for (Schema elemSchema : unionElem.getTypes()) {
      String fieldTag = fieldSchema.parameters() != null ? fieldSchema.parameters()
          .get(PROTOBUF_TYPE_TAG) : null;
      int tag = fieldTag != null ? Integer.parseInt(fieldTag) : 0;

      FieldDefinition.Builder fieldDef = fieldDefinitionFromConnectSchema(
          ctx,
          schema,
          message,
          elemSchema,
          field.name()),
          tag
      );
      if (fieldDef != null) {
        fieldDef.setOneofIndex(oneof.getIdx());
        oneof.addField(fieldDef.build());
      }
    }

     */
  }

  private FieldDefinition.Builder fieldDefinitionFromConnectSchema(
      FromConnectContext ctx,
      DynamicSchema.Builder schema,
      MessageDefinition.Builder message,
      Schema fieldSchema,
      String name,
      Object defaultVal,
      int tag
  ) {
    String label = getLabel(fieldSchema);
    if (fieldSchema.getType() == Schema.Type.ARRAY) {
      fieldSchema = fieldSchema.getValueType();
    }
    Map<String, String> params = new HashMap<>();
    String type = dataTypeFromConnectSchema(ctx, fieldSchema, name, params);
    if (fieldSchema.getType() == Schema.Type.STRUCT) {
      String fieldSchemaName = fieldSchema.getName();
      if (isUnionSchema(fieldSchema)) {
        // TODO RAY
        /*
        String unionName = generalizedSumTypeSupport
            ? fieldSchema.parameters().get(GENERALIZED_TYPE_UNION)
            : getUnqualifiedName(
                ctx, fieldSchemaName.substring(PROTOBUF_TYPE_UNION_PREFIX.length()));
        oneofDefinitionFromConnectSchema(ctx, schema, message, fieldSchema, unionName);
         */
        return null;
      } else {
        if (!ctx.contains(message, type)) {
          ctx.add(message, type);
          message.addMessageDefinition(messageDefinitionFromConnectSchema(
              ctx,
              schema,
              type,
              fieldSchema
          ));
        }
      }
    } else if (fieldSchema.getType() == Schema.Type.MAP) {
      message.addMessageDefinition(
          mapDefinitionFromConnectSchema(ctx, schema, type, fieldSchema));
      /*
    } else if (fieldSchema.parameters() != null
        && (fieldSchema.parameters().containsKey(GENERALIZED_TYPE_ENUM)
            || fieldSchema.parameters().containsKey(PROTOBUF_TYPE_ENUM))) {
      String enumName = getUnqualifiedName(ctx, fieldSchema.name());
      if (!message.containsEnum(enumName)) {
        message.addEnumDefinition(enumDefinitionFromConnectSchema(ctx, schema, fieldSchema));
      }
       */
    } else {
      DynamicSchema dynamicSchema = typeToDynamicSchema(type);
      if (dynamicSchema != null) {
        schema.addSchema(dynamicSchema);
        schema.addDependency(dynamicSchema.getFileDescriptorProto().getName());
      }
    }
    FieldDefinition.Builder builder = FieldDefinition.newBuilder(new Context(), name, tag, type);
    if (label != null) {
      builder.setLabel(label);
    }
    if (defaultVal != null) {
      builder.setDefaultValue(defaultVal.toString());
    }
    builder.setMeta(new ProtobufMeta(null, params, null));
    return builder;
  }

  private String getLabel(Schema fieldSchema) {
    String label = null;
    if (fieldSchema.getType() == Schema.Type.ARRAY) {
      label = "repeated";
    } else if (fieldSchema.getType() == Schema.Type.MAP) {
      label = "repeated";
    } else if (fieldSchema.isOptional()) {
      label = "optional";
    }
    return label;
  }

  private Descriptor typeToDescriptor(String type) {
    DynamicSchema dynamicSchema = typeToDynamicSchema(type);
    if (dynamicSchema == null) {
      return null;
    }
    return dynamicSchema.getMessageDescriptor(type);
  }

  private DynamicSchema typeToDynamicSchema(String type) {
    ProtobufSchema dep;
    switch (type) {
      case PROTOBUF_DECIMAL_TYPE:
        dep = new ProtobufSchema(
            io.confluent.protobuf.type.Decimal.getDescriptor());
        return dep.toDynamicSchema(PROTOBUF_DECIMAL_LOCATION);
      case PROTOBUF_DATE_TYPE:
        dep = new ProtobufSchema(com.google.type.Date.getDescriptor());
        return dep.toDynamicSchema(PROTOBUF_DATE_LOCATION);
      case PROTOBUF_TIME_TYPE:
        dep = new ProtobufSchema(com.google.type.TimeOfDay.getDescriptor());
        return dep.toDynamicSchema(PROTOBUF_TIME_LOCATION);
      case PROTOBUF_TIMESTAMP_TYPE:
        dep = new ProtobufSchema(com.google.protobuf.Timestamp.getDescriptor());
        return dep.toDynamicSchema(PROTOBUF_TIMESTAMP_LOCATION);
      case PROTOBUF_DOUBLE_WRAPPER_TYPE:
        dep = new ProtobufSchema(com.google.protobuf.DoubleValue.getDescriptor());
        return dep.toDynamicSchema(PROTOBUF_WRAPPER_LOCATION);
      case PROTOBUF_FLOAT_WRAPPER_TYPE:
        dep = new ProtobufSchema(com.google.protobuf.FloatValue.getDescriptor());
        return dep.toDynamicSchema(PROTOBUF_WRAPPER_LOCATION);
      case PROTOBUF_INT64_WRAPPER_TYPE:
        dep = new ProtobufSchema(com.google.protobuf.Int64Value.getDescriptor());
        return dep.toDynamicSchema(PROTOBUF_WRAPPER_LOCATION);
      case PROTOBUF_UINT64_WRAPPER_TYPE:
        dep = new ProtobufSchema(com.google.protobuf.UInt64Value.getDescriptor());
        return dep.toDynamicSchema(PROTOBUF_WRAPPER_LOCATION);
      case PROTOBUF_INT32_WRAPPER_TYPE:
        dep = new ProtobufSchema(com.google.protobuf.Int32Value.getDescriptor());
        return dep.toDynamicSchema(PROTOBUF_WRAPPER_LOCATION);
      case PROTOBUF_UINT32_WRAPPER_TYPE:
        dep = new ProtobufSchema(com.google.protobuf.UInt32Value.getDescriptor());
        return dep.toDynamicSchema(PROTOBUF_WRAPPER_LOCATION);
      case PROTOBUF_BOOL_WRAPPER_TYPE:
        dep = new ProtobufSchema(com.google.protobuf.BoolValue.getDescriptor());
        return dep.toDynamicSchema(PROTOBUF_WRAPPER_LOCATION);
      case PROTOBUF_STRING_WRAPPER_TYPE:
        dep = new ProtobufSchema(com.google.protobuf.StringValue.getDescriptor());
        return dep.toDynamicSchema(PROTOBUF_WRAPPER_LOCATION);
      case PROTOBUF_BYTES_WRAPPER_TYPE:
        dep = new ProtobufSchema(com.google.protobuf.BytesValue.getDescriptor());
        return dep.toDynamicSchema(PROTOBUF_WRAPPER_LOCATION);
      default:
        return null;
    }
  }

  private MessageDefinition mapDefinitionFromConnectSchema(
      FromConnectContext ctx, DynamicSchema.Builder schema, String name, Schema mapElem
  ) {
    MessageDefinition.Builder map = MessageDefinition.newBuilder(name);
    FieldDefinition.Builder key = fieldDefinitionFromConnectSchema(
        ctx,
        schema,
        map,
        mapElem.getKeyType(),
        KEY_FIELD,
        null,
        1
    );
    map.addField(key.build());
    FieldDefinition.Builder val = fieldDefinitionFromConnectSchema(
        ctx,
        schema,
        map,
        mapElem.getValueType(),
        VALUE_FIELD,
        null,
        2
    );
    map.addField(val.build());
    return map.build();
  }

  private EnumDefinition enumDefinitionFromConnectSchema(
      FromConnectContext ctx,
      DynamicSchema.Builder schema,
      Schema enumElem
  ) {
    String enumName = getUnqualifiedName(ctx, enumElem.getName());
    EnumDefinition.Builder enumBuilder = EnumDefinition.newBuilder(enumName);
    // TODO RAY
    /*
    String paramName = generalizedSumTypeSupport ? GENERALIZED_TYPE_ENUM : PROTOBUF_TYPE_ENUM;
    for (Map.Entry<String, String> entry : enumElem.parameters().entrySet()) {
      if (entry.getKey().startsWith(paramName + ".")) {
        String name = entry.getKey().substring(paramName.length() + 1);
        int tag = Integer.parseInt(entry.getValue());
        enumBuilder.addValue(name, tag);
      }
    }

     */
    return enumBuilder.build();
  }

  private String dataTypeFromConnectSchema(
      FromConnectContext ctx, Schema schema, String fieldName, Map<String, String> params) {
    if (isDecimalSchema(schema)) {
      if (schema.getProps().hasProps()) {
        String precision = schema.getProp(CONNECT_PRECISION_PROP);
        if (precision != null) {
          params.put(PROTOBUF_PRECISION_PROP, precision);
        }
        String scale = schema.getProp(CONNECT_SCALE_PROP);
        if (scale != null) {
          params.put(PROTOBUF_SCALE_PROP, scale);
        }
      }
      return PROTOBUF_DECIMAL_TYPE;
    } else if (isDateSchema(schema)) {
      return PROTOBUF_DATE_TYPE;
    } else if (isTimeSchema(schema)) {
      return PROTOBUF_TIME_TYPE;
    } else if (isTimestampSchema(schema)) {
      return PROTOBUF_TIMESTAMP_TYPE;
    }
    String defaultType;
    switch (schema.getType()) {
      case INT8:
        params.put(CONNECT_TYPE_PROP, CONNECT_TYPE_INT8);
        return useWrapperForNullables && schema.isOptional()
            ? PROTOBUF_INT32_WRAPPER_TYPE : FieldDescriptor.Type.INT32.toString().toLowerCase();
      case INT16:
        params.put(CONNECT_TYPE_PROP, CONNECT_TYPE_INT16);
        return useWrapperForNullables && schema.isOptional()
            ? PROTOBUF_INT32_WRAPPER_TYPE : FieldDescriptor.Type.INT32.toString().toLowerCase();
      case INT32:
        if (schema.getProp(PROTOBUF_TYPE_ENUM) != null) {
          return schema.getProp(PROTOBUF_TYPE_ENUM);
        }
        defaultType = FieldDescriptor.Type.INT32.toString().toLowerCase();
        if (schema.getProp(PROTOBUF_TYPE_PROP) != null) {
          defaultType = schema.getProp(PROTOBUF_TYPE_PROP);
        }
        return useWrapperForNullables && schema.isOptional()
            ? PROTOBUF_INT32_WRAPPER_TYPE : defaultType;
      case INT64:
        defaultType = FieldDescriptor.Type.INT64.toString().toLowerCase();
        if (schema.getProp(PROTOBUF_TYPE_PROP) != null) {
          defaultType = schema.getProp(PROTOBUF_TYPE_PROP);
        }
        String wrapperType;
        switch (defaultType) {
          case "uint32":
          case "fixed32":
            wrapperType = PROTOBUF_UINT32_WRAPPER_TYPE;
            break;
          case "uint64":
          case "fixed64":
            wrapperType = PROTOBUF_UINT64_WRAPPER_TYPE;
            break;
          default:
            wrapperType = PROTOBUF_INT64_WRAPPER_TYPE;
        }
        return useWrapperForNullables && schema.isOptional()
            ? wrapperType : defaultType;
      case FLOAT32:
        return useWrapperForNullables && schema.isOptional()
            ? PROTOBUF_FLOAT_WRAPPER_TYPE : FieldDescriptor.Type.FLOAT.toString().toLowerCase();
      case FLOAT64:
        return useWrapperForNullables && schema.isOptional()
            ? PROTOBUF_DOUBLE_WRAPPER_TYPE : FieldDescriptor.Type.DOUBLE.toString().toLowerCase();
      case BOOLEAN:
        return useWrapperForNullables && schema.isOptional()
            ? PROTOBUF_BOOL_WRAPPER_TYPE : FieldDescriptor.Type.BOOL.toString().toLowerCase();
      case STRING:
        return useWrapperForNullables && schema.isOptional()
            ? PROTOBUF_STRING_WRAPPER_TYPE : FieldDescriptor.Type.STRING.toString().toLowerCase();
      case BYTES:
        return useWrapperForNullables && schema.isOptional()
            ? PROTOBUF_BYTES_WRAPPER_TYPE : FieldDescriptor.Type.BYTES.toString().toLowerCase();
      case ARRAY:
        // Array should not occur here
        throw new IllegalArgumentException("Array cannot be nested");
      case MAP:
        return ProtobufSchema.toMapEntry(getUnqualifiedName(ctx, schema.getName()));
      case STRUCT:
        String name = getUnqualifiedName(ctx, schema.getName());
        if (name.equals(fieldName)) {
          // Can't have message types and fields with same name, add suffix to message type
          name += "Message";
        }
        return name;
      default:
        throw new SchemaRuntimeException("Unknown schema type: " + schema.getType());
    }
  }

  private boolean isDecimalSchema(Schema schema) {
    // TODO RAY
    //return Decimal.LOGICAL_NAME.equals(schema.name());
    return false;
  }

  private boolean isDateSchema(Schema schema) {
    /*
    return Date.LOGICAL_NAME.equals(schema.name());

     */
    return false;
  }

  private boolean isTimeSchema(Schema schema) {
    /*
    return Time.LOGICAL_NAME.equals(schema.name());

     */
    return false;
  }

  private boolean isTimestampSchema(Schema schema) {
    /*
    return Timestamp.LOGICAL_NAME.equals(schema.name());

     */
    return false;
  }

  private static boolean isUnionSchema(Schema schema) {
    return false;
    /*
    return (schema.name() != null && schema.name().startsWith(PROTOBUF_TYPE_UNION))
        || ConnectUnion.isUnion(schema);

     */
  }

  private String unionFieldName(OneofDescriptor oneofDescriptor) {
    String name = oneofDescriptor.getName();
    if (generateIndexForUnions) {
      name += "_" + oneofDescriptor.getIndex();
    }
    return name;
  }

  private boolean isPrimitiveOrRepeated(FieldDescriptor fieldDescriptor) {
    return fieldDescriptor.getType() != FieldDescriptor.Type.MESSAGE
        || fieldDescriptor.isRepeated();
  }

  private boolean isOptional(FieldDescriptor fieldDescriptor) {
    return fieldDescriptor.toProto().getProto3Optional()
        || (supportOptionalForProto2 && hasOptionalKeyword(fieldDescriptor));
  }

  // copied from Descriptors.java since it is not public
  private boolean hasOptionalKeyword(FieldDescriptor fieldDescriptor) {
    return fieldDescriptor.toProto().getProto3Optional()
        || (getEdition(fieldDescriptor.getFile()) == Edition.EDITION_PROTO2
        && fieldDescriptor.isOptional()
        && fieldDescriptor.getContainingOneof() == null);
  }

  // copied from Descriptors.java since it is not public
  private DescriptorProtos.Edition getEdition(FileDescriptor file) {
    switch (file.toProto().getSyntax()) {
      case "editions":
        return file.toProto().getEdition();
      case "proto3":
        return Edition.EDITION_PROTO3;
      default:
        return Edition.EDITION_PROTO2;
    }
  }

  private boolean isProto3Optional(FieldDescriptor fieldDescriptor) {
    return fieldDescriptor.toProto().getProto3Optional();
  }

  public NativeSchema toNativeSchema(ProtobufSchema schema) {
    if (schema == null) {
      return null;
    }
    Pair<String, ProtobufSchema> cacheKey = new Pair<>(schema.name(), schema);
    NativeSchema cachedSchema = toConnectSchemaCache.get(cacheKey);
    if (cachedSchema != null) {
      return cachedSchema;
    }
    Descriptor descriptor = schema.toDescriptor();
    ToConnectContext ctx = new ToConnectContext();
    Schema nativeSchema = toNativeSchema(ctx, descriptor, schema.version());
    NativeSchema resultSchema = new NativeSchema(nativeSchema);
    toConnectSchemaCache.put(cacheKey, resultSchema);
    return resultSchema;
  }

  private Schema toNativeSchema(
      ToConnectContext ctx, Descriptor descriptor, Integer version) {
    Schema result = toUnwrappedSchema(descriptor);
    if (result != null) {
      return result;
    }
    String name = enhancedSchemaSupport ? descriptor.getFullName() : descriptor.getName();
    FieldAssembler<Schema> builder = SchemaBuilder.builder().struct(name).fields();
    List<OneofDescriptor> oneOfDescriptors = descriptor.getRealOneofs();
    for (OneofDescriptor oneOfDescriptor : oneOfDescriptors) {
      if (flattenUnions) {
        List<FieldDescriptor> fieldDescriptors = oneOfDescriptor.getFields();
        for (FieldDescriptor fieldDescriptor : fieldDescriptors) {
          builder = toNativeSchema(ctx, fieldDescriptor, builder.name(fieldDescriptor.getName()));
        }
      } else {
        String unionFieldName = unionFieldName(oneOfDescriptor);
        // TODO RAY fix default
        builder = builder.name(unionFieldName).type(toNativeSchema(ctx, oneOfDescriptor)).noDefault();
      }
    }
    List<FieldDescriptor> fieldDescriptors = descriptor.getFields();
    for (FieldDescriptor fieldDescriptor : fieldDescriptors) {
      OneofDescriptor oneOfDescriptor = fieldDescriptor.getRealContainingOneof();
      if (oneOfDescriptor != null) {
        // Already added field as oneof
        continue;
      }
      // TODO RAY fix default
      builder = toNativeSchema(ctx, fieldDescriptor, builder.name(fieldDescriptor.getName()));
    }
    return builder.endStruct();
  }

  private Schema toNativeSchema(ToConnectContext ctx, OneofDescriptor descriptor) {
    // TODO RAY
    /*
    SchemaBuilder builder = SchemaBuilder.struct();
    if (generalizedSumTypeSupport) {
      String name = descriptor.getName();
      builder.name(name);
      builder.parameter(GENERALIZED_TYPE_UNION, name);
    } else {
      String name = enhancedSchemaSupport ? descriptor.getFullName() : descriptor.getName();
      builder.name(PROTOBUF_TYPE_UNION_PREFIX + name);
    }
    List<FieldDescriptor> fieldDescriptors = descriptor.getFields();
    for (FieldDescriptor fieldDescriptor : fieldDescriptors) {
      builder.field(fieldDescriptor.getName(), toNativeSchema(ctx, fieldDescriptor));
    }
    builder.optional();
    return builder.build();

     */
    return null;
  }

  private FieldAssembler<Schema> toNativeSchema(
      ToConnectContext ctx, FieldDescriptor descriptor, FieldBuilder<Schema> builder) {
    builder = builder.prop(PROTOBUF_TYPE_TAG, String.valueOf(descriptor.getNumber()));

    switch (descriptor.getType()) {
      case INT32:
      case SINT32:
      case SFIXED32: {
        if (descriptor.getType() != FieldDescriptor.Type.INT32) {
          builder = builder.prop(PROTOBUF_TYPE_PROP, descriptor.getType().toString().toLowerCase());
        }
        if (descriptor.getOptions().hasExtension(MetaProto.fieldMeta)) {
          Meta fieldMeta = descriptor.getOptions().getExtension(MetaProto.fieldMeta);
          Map<String, String> params = fieldMeta.getParamsMap();
          if (params != null) {
            String connectType = params.get(CONNECT_TYPE_PROP);
            if (CONNECT_TYPE_INT8.equals(connectType)) {
              if (hasOptionalKeyword(descriptor)) {
                return builder.type().optional().byteType();
              } else {
                return builder.type().byteType().noDefault();
              }
            } else if (CONNECT_TYPE_INT16.equals(connectType)) {
              if (hasOptionalKeyword(descriptor)) {
                return builder.type().optional().shortType();
              } else {
                return builder.type().shortType().noDefault();
              }
            }
          }
        }
        if (hasOptionalKeyword(descriptor)) {
          return builder.type().optional().intType();
        } else {
          return builder.type().intType().noDefault();
        }
      }

      case UINT32:
      case FIXED32:
      case INT64:
      case UINT64:
      case SINT64:
      case FIXED64:
      case SFIXED64: {
        if (descriptor.getType() != FieldDescriptor.Type.INT64) {
          builder = builder.prop(PROTOBUF_TYPE_PROP, descriptor.getType().toString().toLowerCase());
        }
        if (hasOptionalKeyword(descriptor)) {
          return builder.type().optional().longType();
        } else {
          return builder.type().longType().noDefault();
        }
      }

      case FLOAT: {
        if (hasOptionalKeyword(descriptor)) {
          return builder.type().optional().floatType();
        } else {
          return builder.type().floatType().noDefault();
        }
      }

      case DOUBLE: {
        if (hasOptionalKeyword(descriptor)) {
          return builder.type().optional().doubleType();
        } else {
          return builder.type().doubleType().noDefault();
        }
      }

      case BOOL: {
        if (hasOptionalKeyword(descriptor)) {
          return builder.type().optional().booleanType();
        } else {
          return builder.type().booleanType().noDefault();
        }
      }

      case STRING:
        if (hasOptionalKeyword(descriptor)) {
          return builder.type().optional().stringType();
        } else {
          return builder.type().stringType().noDefault();
        }

      case BYTES:
        if (hasOptionalKeyword(descriptor)) {
          return builder.type().optional().bytesType();
        } else {
          return builder.type().bytesType().noDefault();
        }

      case ENUM:
        EnumDescriptor enumDescriptor = descriptor.getEnumType();
        String name = enhancedSchemaSupport
            ? enumDescriptor.getFullName() : enumDescriptor.getName();
        // TODO RAY
        String paramName = GENERALIZED_TYPE_ENUM;
        builder = builder.prop(paramName, enumDescriptor.getName());
        String[] symbols = new String[enumDescriptor.getValues().size()];
        for (int i = 0; i < enumDescriptor.getValues().size(); i++) {
          EnumValueDescriptor enumValueDesc = enumDescriptor.getValues().get(i);
          String enumSymbol = enumValueDesc.getName();
          String enumTag = String.valueOf(enumValueDesc.getNumber());
          builder = builder.prop(paramName + "." + enumSymbol, enumTag);
          symbols[i] = enumSymbol;
        }
        if (hasOptionalKeyword(descriptor)) {
          return builder.type().optional().enumeration(name).symbols(symbols);
        } else {
          return builder.type().enumeration(name).symbols(symbols).noDefault();
        }

      case MESSAGE: {
        String fullName = descriptor.getMessageType().getFullName();
        switch (fullName) {
          /*
          case PROTOBUF_DECIMAL_TYPE:
            Integer precision = null;
            int scale = 0;
            if (descriptor.getOptions().hasExtension(MetaProto.fieldMeta)) {
              Meta fieldMeta = descriptor.getOptions().getExtension(MetaProto.fieldMeta);
              Map<String, String> params = fieldMeta.getParamsMap();
              String precisionStr = params.get(PROTOBUF_PRECISION_PROP);
              if (precisionStr != null) {
                try {
                  precision = Integer.parseInt(precisionStr);
                } catch (NumberFormatException e) {
                  // ignore
                }
              }
              String scaleStr = params.get(PROTOBUF_SCALE_PROP);
              if (scaleStr != null) {
                try {
                  scale = Integer.parseInt(scaleStr);
                } catch (NumberFormatException e) {
                  // ignore
                }
              }
            }
            builder = Decimal.builder(scale);
            if (precision != null) {
              builder.parameter(CONNECT_PRECISION_PROP, precision.toString());
            }
            break;
          case PROTOBUF_DATE_TYPE:
            builder = Date.builder();
            break;
          case PROTOBUF_TIME_TYPE:
            builder = Time.builder();
            break;
          case PROTOBUF_TIMESTAMP_TYPE:
            builder = Timestamp.builder();
            break;

           */
          default:
            return toUnwrappedOrStructSchema(ctx, descriptor, builder);
        }
      }

      default:
        throw new SchemaRuntimeException("Unknown Connect schema type: " + descriptor.getType());
    }
  }

  private FieldAssembler<Schema> toUnwrappedOrStructSchema(
          ToConnectContext ctx, FieldDescriptor descriptor, FieldBuilder<Schema> builder) {
    FieldAssembler<Schema> assembler = toUnwrappedSchema(descriptor.getMessageType(), builder);
    return assembler != null ? assembler : toStructSchema(ctx, descriptor, builder);
  }

  private Schema toUnwrappedSchema(Descriptor descriptor) {
    String fullName = descriptor.getFullName();
    switch (fullName) {
      case PROTOBUF_DOUBLE_WRAPPER_TYPE:
        return SchemaBuilder.builder().doubleType();
      case PROTOBUF_FLOAT_WRAPPER_TYPE:
        return SchemaBuilder.builder().floatType();
      case PROTOBUF_INT64_WRAPPER_TYPE:
        return SchemaBuilder.builder().longType();
      case PROTOBUF_UINT64_WRAPPER_TYPE:
        return SchemaBuilder.builder().longType();
      case PROTOBUF_INT32_WRAPPER_TYPE:
        return SchemaBuilder.builder().intType();
      case PROTOBUF_UINT32_WRAPPER_TYPE:
        return SchemaBuilder.builder().intType();
      case PROTOBUF_BOOL_WRAPPER_TYPE:
        return SchemaBuilder.builder().booleanType();
      case PROTOBUF_STRING_WRAPPER_TYPE:
        return SchemaBuilder.builder().stringType();
      case PROTOBUF_BYTES_WRAPPER_TYPE:
        return SchemaBuilder.builder().bytesType();
      default:
        return null;
    }
  }

  private FieldAssembler<Schema> toUnwrappedSchema(Descriptor descriptor, FieldBuilder<Schema> builder) {
    String fullName = descriptor.getFullName();
    switch (fullName) {
      case PROTOBUF_DOUBLE_WRAPPER_TYPE:
        return builder.type().optional().doubleType();
      case PROTOBUF_FLOAT_WRAPPER_TYPE:
        return builder.type().optional().floatType();
      case PROTOBUF_INT64_WRAPPER_TYPE:
        return builder.type().optional().longType();
      case PROTOBUF_UINT64_WRAPPER_TYPE:
        return builder.type().optional().longType();
      case PROTOBUF_INT32_WRAPPER_TYPE:
        return builder.type().optional().intType();
      case PROTOBUF_UINT32_WRAPPER_TYPE:
        return builder.type().optional().intType();
      case PROTOBUF_BOOL_WRAPPER_TYPE:
        return builder.type().optional().booleanType();
      case PROTOBUF_STRING_WRAPPER_TYPE:
        return builder.type().optional().stringType();
      case PROTOBUF_BYTES_WRAPPER_TYPE:
        return builder.type().optional().bytesType();
      default:
        return null;
    }
  }

  private FieldAssembler<Schema> toStructSchema(ToConnectContext ctx, FieldDescriptor descriptor, FieldBuilder<Schema> builder) {
    // TODO RAY
    /*
    if (isMapDescriptor(descriptor)) {
      return toMapSchema(ctx, descriptor.getMessageType());
    }

     */
    String fullName = descriptor.getMessageType().getFullName();
    // TODO RAY
    /*
    SchemaBuilder builder = ctx.get(fullName);
    if (builder != null) {
      builder = new SchemaWrapper(builder);
    } else {

     */
      Schema s = toNativeSchema(ctx, descriptor.getMessageType(), null);
    //}
    return builder.type(s).noDefault();
  }

  private static boolean isMapDescriptor(
      FieldDescriptor fieldDescriptor
  ) {
    if (!fieldDescriptor.isRepeated()) {
      return false;
    }
    Descriptor descriptor = fieldDescriptor.getMessageType();
    List<FieldDescriptor> fieldDescriptors = descriptor.getFields();
    return descriptor.getName().endsWith(MAP_ENTRY_SUFFIX)
        && fieldDescriptors.size() == 2
        && fieldDescriptors.get(0).getName().equals(KEY_FIELD)
        && fieldDescriptors.get(1).getName().equals(VALUE_FIELD)
        && !fieldDescriptors.get(0).isRepeated()
        && !fieldDescriptors.get(1).isRepeated();
  }

  /*
  private SchemaBuilder toMapSchema(ToConnectContext ctx, Descriptor descriptor) {
    List<FieldDescriptor> fieldDescriptors = descriptor.getFields();
    String name = ProtobufSchema.toMapField(
        enhancedSchemaSupport ? descriptor.getFullName() : descriptor.getName());
    return SchemaBuilder.map(toNativeSchema(ctx, fieldDescriptors.get(0)),
        toNativeSchema(ctx, fieldDescriptors.get(1))
    ).name(name);
  }

   */

  /**
   * Split a full dotted-syntax name into a namespace and a single-component name.
   */
  private String[] splitName(String fullName) {
    String[] result = new String[2];
    if (fullName == null || fullName.isEmpty()) {
      result[0] = null;
      result[1] = fullName;
      return result;
    }
    String[] parts = fullName.split("\\.");
    for (int i = 0; i < parts.length; i++) {
      parts[i] = scrubName(parts[i]);
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

  /**
   * Strip the namespace from a name.
   */
  private String getUnqualifiedName(FromConnectContext ctx, String name) {
    String fullName = getNameOrDefault(ctx, name);
    int indexLastDot = fullName.lastIndexOf('.');
    String result;
    if (indexLastDot >= 0) {
      result = fullName.substring(indexLastDot + 1);
    } else {
      result = fullName;
    }
    return scrubName(result);
  }

  private String scrubName(String name) {
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
      nameOK = nameOK && (name.charAt(0) < '0' || name.charAt(0) > '9') && name.charAt(0) != '_';
      if (nameOK) {
        return name;
      }

      // String needs to be scrubbed
      String encoded = URLEncoder.encode(name, "UTF-8");
      if ((encoded.charAt(0) >= '0' && encoded.charAt(0) <= '9') || encoded.charAt(0) == '_') {
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

  private String getNameOrDefault(FromConnectContext ctx, String name) {
    return name != null && !name.isEmpty()
           ? name
           : DEFAULT_SCHEMA_NAME + ctx.incrementAndGetNameIndex();
  }

  private interface LogicalTypeConverter {
    Object convert(Schema schema, Object value);
  }

  /**
   * Class that holds the context for performing {@code toConnectSchema}
   */
  private static class ToConnectContext {
    private final Map<String, SchemaBuilder> messageToStructMap;

    public ToConnectContext() {
      this.messageToStructMap = new HashMap<>();
    }

    public SchemaBuilder get(String messageName) {
      return messageToStructMap.get(messageName);
    }

    public void put(String messageName, SchemaBuilder builder) {
      messageToStructMap.put(messageName, builder);
    }
  }

  /**
   * Class that holds the context for performing {@code fromConnectSchema}
   */
  private static class FromConnectContext {
    private final Map<MessageDefinition.Builder, Set<String>> messageNames;
    private int defaultSchemaNameIndex = 0;

    public FromConnectContext() {
      this.messageNames = new IdentityHashMap<>();
    }

    public boolean contains(MessageDefinition.Builder parent, String child) {
      // A reference to a type with the same name as the parent is a reference to the parent
      if (parent.getName().equals(child)) {
        return true;
      }
      Set<String> children = messageNames.get(parent);
      if (child == null || children == null) {
        return false;
      }
      return children.contains(child);
    }

    public void add(MessageDefinition.Builder parent, String child) {
      if (child != null) {
        Set<String> children = messageNames.computeIfAbsent(parent, k -> new HashSet<>());
        children.add(child);
      }
    }

    public int incrementAndGetNameIndex() {
      return ++defaultSchemaNameIndex;
    }
  }

  static class ProtobufSchemaAndValue {

    private final ProtobufSchema schema;
    private final Object value;

    public ProtobufSchemaAndValue(ProtobufSchema schema, Object value) {
      this.schema = schema;
      this.value = value;
    }

    public ProtobufSchema getSchema() {
      return schema;
    }

    public Object getValue() {
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
      ProtobufSchemaAndValue that = (ProtobufSchemaAndValue) o;
      return Objects.equals(schema, that.schema) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(schema, value);
    }
  }
}
