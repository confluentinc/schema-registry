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

package io.confluent.connect.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.util.Timestamps;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.DynamicSchema;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.EnumDefinition;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.MessageDefinition;
import io.confluent.kafka.serializers.protobuf.ProtobufSchemaAndValue;

import static io.confluent.connect.protobuf.ProtobufDataConfig.SCHEMAS_CACHE_SIZE_CONFIG;
import static io.confluent.connect.protobuf.ProtobufDataConfig.SCHEMAS_CACHE_SIZE_DEFAULT;


public class ProtobufData {

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

  public static final String GOOGLE_PROTOBUF_PACKAGE = "google.protobuf";
  public static final String GOOGLE_PROTOBUF_TIMESTAMP_NAME = "Timestamp";
  public static final String GOOGLE_PROTOBUF_TIMESTAMP_FULL_NAME = GOOGLE_PROTOBUF_PACKAGE
      + "."
      + GOOGLE_PROTOBUF_TIMESTAMP_NAME;
  public static final String GOOGLE_PROTOBUF_TIMESTAMP_LOCATION = "google/protobuf/timestamp.proto";

  private int defaultSchemaNameIndex = 0;

  private final Cache<Schema, ProtobufSchema> fromConnectSchemaCache;
  private final Cache<ProtobufSchema, Schema> toConnectSchemaCache;
  private boolean enhancedSchemaSupport;

  public ProtobufData() {
    this(new ProtobufDataConfig.Builder().with(
        SCHEMAS_CACHE_SIZE_CONFIG,
        SCHEMAS_CACHE_SIZE_DEFAULT
    ).build());
  }

  public ProtobufData(int cacheSize) {
    this(new ProtobufDataConfig.Builder().with(SCHEMAS_CACHE_SIZE_CONFIG, cacheSize).build());
  }

  public ProtobufData(ProtobufDataConfig protobufDataConfig) {
    fromConnectSchemaCache =
        new SynchronizedCache<>(new LRUCache<>(protobufDataConfig.schemaCacheSize()));
    toConnectSchemaCache =
        new SynchronizedCache<>(new LRUCache<>(protobufDataConfig.schemaCacheSize()));
    this.enhancedSchemaSupport = protobufDataConfig.isEnhancedProtobufSchemaSupport();
  }

  /**
   * Convert this object, in Connect data format, into an Protobuf object.
   */
  public ProtobufSchemaAndValue fromConnectData(Schema schema, Object value) {
    ProtobufSchema protobufSchema = fromConnectSchema(schema);
    // Reset the default schema name index.
    // Note: the assumption is that default names will be generated in an order in both the
    // schema and data
    // such that the names correspond to each other in the schema and data.
    // TODO try to break this assumption
    defaultSchemaNameIndex = 0;
    return new ProtobufSchemaAndValue(
        protobufSchema,
        fromConnectData(schema, "", value, protobufSchema)
    );
  }

  // Visible for testing
  protected ProtobufSchemaAndValue fromConnectData(SchemaAndValue schemaAndValue) {
    return fromConnectData(schemaAndValue.schema(), schemaAndValue.value());
  }

  private Object fromConnectData(
      Schema schema,
      String scope,
      Object value,
      ProtobufSchema protobufSchema
  ) {
    if (value == null) {
      // Ignore missing values
      return null;
    }

    final Schema.Type schemaType = schema.type();
    try {
      switch (schemaType) {
        case INT8:
        case INT16:
        case INT32: {
          final int intValue = ((Number) value).intValue(); // Check for correct type
          return intValue;
        }

        case INT64: {
          if (isProtobufTimestamp(schema)) {
            final java.util.Date timestamp = (java.util.Date) value;
            return Timestamps.fromMillis(Timestamp.fromLogical(schema, timestamp));
          }

          final long longValue = ((Number) value).longValue(); // Check for correct type
          return longValue;
        }

        case FLOAT32: {
          final float floatValue = ((Number) value).floatValue(); // Check for correct type
          return floatValue;
        }

        case FLOAT64: {
          final double doubleValue = ((Number) value).doubleValue(); // Check for correct type
          return doubleValue;
        }

        case BOOLEAN: {
          final Boolean boolValue = (Boolean) value; // Check for correct type
          return boolValue;
        }

        case STRING: {
          final String stringValue = (String) value; // Check for correct type
          if (schema.parameters() != null && schema.parameters().containsKey(PROTOBUF_TYPE_ENUM)) {
            String enumType = schema.parameters().get(PROTOBUF_TYPE_ENUM);
            String tag = schema.parameters().get(PROTOBUF_TYPE_ENUM_PREFIX + stringValue);
            if (tag != null) {
              return protobufSchema.getEnumValue(scope + enumType, Integer.parseInt(tag));
            }
          }
          return stringValue;
        }

        case BYTES: {
          final ByteBuffer bytesValue = value instanceof byte[]
                                        ? ByteBuffer.wrap((byte[]) value)
                                        : (ByteBuffer) value;
          return ByteString.copyFrom(bytesValue);
        }
        case ARRAY:
          final Collection<?> listValue = (Collection<?>) value;
          if (listValue.isEmpty()) {
            return null;
          }
          List<Object> newListValue = new ArrayList<>();
          for (Object o : listValue) {
            newListValue.add(fromConnectData(schema.valueSchema(), scope, o, protobufSchema));
          }
          return newListValue;
        case MAP:
          final Map<?, ?> mapValue = (Map<?, ?>) value;
          String mapName = getUnqualifiedName(schema.name());
          String scopedMapName = scope + ProtobufSchema.toMapEntry(mapName);
          List<Message> newMapValue = new ArrayList<>();
          for (Map.Entry<?, ?> mapEntry : mapValue.entrySet()) {
            DynamicMessage.Builder mapBuilder = protobufSchema.newMessageBuilder(scopedMapName);
            if (mapBuilder == null) {
              throw new IllegalStateException("Invalid message name: " + scopedMapName);
            }
            Descriptor mapDescriptor = mapBuilder.getDescriptorForType();
            final FieldDescriptor keyDescriptor = mapDescriptor.findFieldByName(KEY_FIELD);
            final FieldDescriptor valueDescriptor = mapDescriptor.findFieldByName(VALUE_FIELD);
            Object entryKey = fromConnectData(
                schema.keySchema(),
                scopedMapName + ".",
                mapEntry.getKey(),
                protobufSchema
            );
            Object entryValue = fromConnectData(
                schema.valueSchema(),
                scopedMapName + ".",
                mapEntry.getValue(),
                protobufSchema
            );
            mapBuilder.setField(keyDescriptor, entryKey);
            mapBuilder.setField(valueDescriptor, entryValue);
            newMapValue.add(mapBuilder.build());
          }
          return newMapValue;
        case STRUCT:
          final Struct struct = (Struct) value;
          if (!struct.schema().equals(schema)) {
            throw new DataException("Mismatching struct schema");
          }
          String structName = schema.name();
          //This handles the inverting of a union which is held as a struct, where each field is
          // one of the union types.
          if (structName != null && structName.startsWith(PROTOBUF_TYPE_UNION_PREFIX)) {
            for (Field field : schema.fields()) {
              Object object = struct.get(field);
              if (object != null) {
                return new Pair(field.name(),
                    fromConnectData(field.schema(), scope, object, protobufSchema)
                );
              }
            }
            throw new DataException("Cannot find non-null field");
          } else {
            String scopedStructName = scope + getUnqualifiedName(structName);
            DynamicMessage.Builder messageBuilder =
                protobufSchema.newMessageBuilder(scopedStructName);
            if (messageBuilder == null) {
              throw new DataException("Invalid message name: " + scopedStructName);
            }
            for (Field field : schema.fields()) {
              Object fieldValue = fromConnectData(
                  field.schema(),
                  scopedStructName + ".",
                  struct.get(field),
                  protobufSchema
              );
              if (fieldValue != null) {
                FieldDescriptor fieldDescriptor;
                if (fieldValue instanceof Pair) {
                  Pair<String, Object> union = (Pair<String, Object>) fieldValue;
                  fieldDescriptor = messageBuilder.getDescriptorForType()
                      .findFieldByName(union.getKey());
                  fieldValue = union.getValue();
                } else {
                  fieldDescriptor = messageBuilder.getDescriptorForType()
                      .findFieldByName(field.name());
                }
                if (fieldDescriptor == null) {
                  throw new DataException("Cannot find field with name " + field.name());
                }
                messageBuilder.setField(fieldDescriptor, fieldValue);
              }
            }
            return messageBuilder.build();
          }

        default:
          throw new DataException("Unknown schema type: " + schema.type());
      }
    } catch (ClassCastException e) {
      throw new DataException("Invalid type for " + schema.type() + ": " + value.getClass());
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

  public ProtobufSchema fromConnectSchema(Schema schema) {
    if (schema == null) {
      return null;
    }
    ProtobufSchema cachedSchema = fromConnectSchemaCache.get(schema);
    if (cachedSchema != null) {
      return cachedSchema;
    }
    String name = schema.name();
    if (name == null) {
      name = DEFAULT_SCHEMA_NAME + "1";
    }
    ProtobufSchema resultSchema = new ProtobufSchema(
        rawSchemaFromConnectSchema(schema).getMessageDescriptor(name)
    );
    fromConnectSchemaCache.put(schema, resultSchema);
    return resultSchema;
  }

  /*
   * DynamicSchema is used as a temporary helper class and should not be exposed in the API.
   */
  private DynamicSchema rawSchemaFromConnectSchema(Schema rootElem) {
    if (rootElem.type() != Schema.Type.STRUCT) {
      throw new IllegalArgumentException("Unsupported root schema of type " + rootElem.type());
    }
    try {
      DynamicSchema.Builder schema = DynamicSchema.newBuilder();
      schema.setSyntax(ProtobufSchema.PROTO3);
      String fullName = getNameOrDefault(rootElem.name());
      String[] split = splitName(fullName);
      String namespace = split[0];
      String name = split[1];
      if (namespace != null) {
        schema.setPackage(namespace);
      }
      FromConnectContext ctx = new FromConnectContext();
      ctx.add(fullName);
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
    for (Field field : messageElem.fields()) {
      Schema fieldSchema = field.schema();
      String fieldTag = fieldSchema.parameters() != null ? fieldSchema.parameters()
          .get(PROTOBUF_TYPE_TAG) : null;
      int tag = fieldTag != null ? Integer.parseInt(fieldTag) : index++;
      FieldDefinition fieldDef = fieldDefinitionFromConnectSchema(
          ctx,
          schema,
          message,
          fieldSchema,
          field.name(),
          tag
      );
      if (fieldDef != null) {
        message.addField(fieldDef.getLabel(),
            fieldDef.getType(),
            fieldDef.getName(),
            fieldDef.getNum(),
            fieldDef.getDefaultVal()
        );
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
    MessageDefinition.OneofBuilder oneof = message.addOneof(unionName);
    for (Field field : unionElem.fields()) {
      Schema fieldSchema = field.schema();
      String fieldTag = fieldSchema.parameters() != null ? fieldSchema.parameters()
          .get(PROTOBUF_TYPE_TAG) : null;
      int tag = fieldTag != null ? Integer.parseInt(fieldTag) : 0;
      FieldDefinition fieldDef = fieldDefinitionFromConnectSchema(
          ctx,
          schema,
          message,
          field.schema(),
          field.name(),
          tag
      );
      if (fieldDef != null) {
        oneof.addField(
            fieldDef.getType(),
            fieldDef.getName(),
            fieldDef.getNum(),
            fieldDef.getDefaultVal()
        );
      }
    }
  }

  private FieldDefinition fieldDefinitionFromConnectSchema(
      FromConnectContext ctx,
      DynamicSchema.Builder schema,
      MessageDefinition.Builder message,
      Schema fieldSchema,
      String name,
      int tag
  ) {
    try {
      String label = fieldSchema.isOptional() ? "optional" : "required";
      if (fieldSchema.type() == Schema.Type.ARRAY) {
        label = "repeated";
        fieldSchema = fieldSchema.valueSchema();
      } else if (fieldSchema.type() == Schema.Type.MAP) {
        label = "repeated";
      }
      String type = dataTypeFromConnectSchema(fieldSchema);
      if (fieldSchema.type() == Schema.Type.STRUCT) {
        String fieldSchemaName = fieldSchema.name();
        if (fieldSchemaName != null && fieldSchemaName.startsWith(PROTOBUF_TYPE_UNION_PREFIX)) {
          String unionName =
              getUnqualifiedName(fieldSchemaName.substring(PROTOBUF_TYPE_UNION_PREFIX.length()));
          oneofDefinitionFromConnectSchema(ctx, schema, message, fieldSchema, unionName);
          return null;
        } else {
          if (!ctx.contains(fieldSchemaName)) {
            ctx.add(fieldSchemaName);
            message.addMessageDefinition(messageDefinitionFromConnectSchema(
                ctx,
                schema,
                type,
                fieldSchema
            ));
          }
        }
      } else if (fieldSchema.type() == Schema.Type.MAP) {
        message.addMessageDefinition(
            mapDefinitionFromConnectSchema(ctx, schema, type, fieldSchema));
      } else if (fieldSchema.parameters() != null && fieldSchema.parameters()
          .containsKey(PROTOBUF_TYPE_ENUM)) {
        message.addEnumDefinition(enumDefinitionFromConnectSchema(schema, fieldSchema));
      } else if (type.equals(GOOGLE_PROTOBUF_TIMESTAMP_FULL_NAME)) {
        DynamicSchema.Builder timestampSchema = DynamicSchema.newBuilder();
        timestampSchema.setSyntax(ProtobufSchema.PROTO3);
        timestampSchema.setName(GOOGLE_PROTOBUF_TIMESTAMP_LOCATION);
        timestampSchema.setPackage(GOOGLE_PROTOBUF_PACKAGE);
        timestampSchema.addMessageDefinition(timestampDefinition());
        schema.addSchema(timestampSchema.build());
        schema.addDependency(GOOGLE_PROTOBUF_TIMESTAMP_LOCATION);
      }
      Object defaultVal = fieldSchema.defaultValue();
      return new FieldDefinition(
          label,
          type,
          name,
          tag,
          defaultVal != null ? defaultVal.toString() : null
      );
    } catch (Descriptors.DescriptorValidationException e) {
      throw new IllegalStateException(e);
    }
  }

  static class FieldDefinition {
    private final String label;
    private final String type;
    private final String name;
    private final int num;
    private final String defaultVal;

    public FieldDefinition(String label, String type, String name, int num, String defaultVal) {
      this.label = label;
      this.type = type;
      this.name = name;
      this.num = num;
      this.defaultVal = defaultVal;
    }

    public String getType() {
      return type;
    }

    public String getName() {
      return name;
    }

    public int getNum() {
      return num;
    }

    public String getDefaultVal() {
      return defaultVal;
    }

    public String getLabel() {
      return label;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FieldDefinition field = (FieldDefinition) o;
      return num == field.num && Objects.equals(label, field.label) && Objects.equals(
          type,
          field.type
      ) && Objects.equals(name, field.name) && Objects.equals(defaultVal, field.defaultVal);
    }

    @Override
    public int hashCode() {
      return Objects.hash(label, type, name, num, defaultVal);
    }
  }

  private MessageDefinition mapDefinitionFromConnectSchema(
      FromConnectContext ctx, DynamicSchema.Builder schema, String name, Schema mapElem
  ) {
    MessageDefinition.Builder map = MessageDefinition.newBuilder(name);
    FieldDefinition key = fieldDefinitionFromConnectSchema(
        ctx,
        schema,
        map,
        mapElem.keySchema(),
        KEY_FIELD,
        1
    );
    map.addField(key.getLabel(), key.getType(), key.getName(), key.getNum(), key.getDefaultVal());
    FieldDefinition val = fieldDefinitionFromConnectSchema(
        ctx,
        schema,
        map,
        mapElem.valueSchema(),
        VALUE_FIELD,
        2
    );
    map.addField(val.getLabel(), val.getType(), val.getName(), val.getNum(), val.getDefaultVal());
    return map.build();
  }

  private EnumDefinition enumDefinitionFromConnectSchema(
      DynamicSchema.Builder schema,
      Schema enumElem
  ) {
    String enumName = getUnqualifiedName(enumElem.name());
    EnumDefinition.Builder enumer = EnumDefinition.newBuilder(enumName);
    for (Map.Entry<String, String> entry : enumElem.parameters().entrySet()) {
      if (entry.getKey().startsWith(PROTOBUF_TYPE_ENUM_PREFIX)) {
        String name = entry.getKey().substring(PROTOBUF_TYPE_ENUM_PREFIX.length());
        int tag = Integer.parseInt(entry.getValue());
        enumer.addValue(name, tag);
      }
    }
    return enumer.build();
  }

  private String dataTypeFromConnectSchema(Schema schema) {
    switch (schema.type()) {
      case INT8:
      case INT16:
      case INT32:
        return FieldDescriptor.Type.INT32.toString().toLowerCase();
      case INT64:
        if (isProtobufTimestamp(schema)) {
          return GOOGLE_PROTOBUF_TIMESTAMP_FULL_NAME;
        }
        return FieldDescriptor.Type.INT64.toString().toLowerCase();
      case FLOAT32:
        return FieldDescriptor.Type.FLOAT.toString().toLowerCase();
      case FLOAT64:
        return FieldDescriptor.Type.DOUBLE.toString().toLowerCase();
      case BOOLEAN:
        return FieldDescriptor.Type.BOOL.toString().toLowerCase();
      case STRING:
        if (schema.parameters() != null && schema.parameters().containsKey(PROTOBUF_TYPE_ENUM)) {
          return schema.parameters().get(PROTOBUF_TYPE_ENUM);
        }
        return FieldDescriptor.Type.STRING.toString().toLowerCase();
      case BYTES:
        return FieldDescriptor.Type.BYTES.toString().toLowerCase();
      case ARRAY:
        // Array should not occur here
        throw new IllegalArgumentException("Array cannot be nested");
      case MAP:
        return ProtobufSchema.toMapEntry(getUnqualifiedName(schema.name()));
      case STRUCT:
        return getUnqualifiedName(schema.name());
      default:
        throw new DataException("Unknown schema type: " + schema.type());
    }
  }

  private boolean isProtobufTimestamp(Schema schema) {
    return Timestamp.SCHEMA.name().equals(schema.name());
  }

  public SchemaAndValue toConnectData(ProtobufSchema protobufSchema, Message message) {
    if (message == null) {
      return SchemaAndValue.NULL;
    }

    Schema schema = toConnectSchema(protobufSchema);
    return new SchemaAndValue(schema, toConnectData(schema, message));
  }

  // Visible for testing
  @SuppressWarnings("unchecked")
  protected Object toConnectData(Schema schema, Object value) {
    try {
      if (value == null) {
        return null;
      }
      if (isProtobufTimestamp(schema)) {
        Message message = (Message) value;

        long seconds = 0L;
        int nanos = 0;
        for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
          if (entry.getKey().getName().equals("seconds")) {
            seconds = ((Number) entry.getValue()).longValue();
          } else if (entry.getKey().getName().equals("nanos")) {
            nanos = ((Number) entry.getValue()).intValue();
          }
        }
        com.google.protobuf.Timestamp timestamp = com.google.protobuf.Timestamp.newBuilder()
            .setSeconds(seconds)
            .setNanos(nanos)
            .build();
        return Timestamp.toLogical(schema, Timestamps.toMillis(timestamp));
      }

      Object converted = null;
      switch (schema.type()) {
        case INT8:
        case INT16:
        case INT32:
          converted = ((Number) value).intValue();
          break;
        case INT64:
          long longValue;
          if (value instanceof Long) {
            longValue = (Long) value;
          } else {
            longValue = Integer.toUnsignedLong(((Number) value).intValue());
          }
          converted = longValue;
          break;
        case FLOAT32:
          converted = ((Number) value).floatValue();
          break;
        case FLOAT64:
          converted = ((Number) value).doubleValue();
          break;
        case BOOLEAN:
          converted = (Boolean) value;
          break;
        case STRING:
          if (value instanceof String) {
            converted = value;
          } else if (value instanceof CharSequence
              || value instanceof Enum
              || value instanceof EnumValueDescriptor) {
            converted = value.toString();
          } else {
            throw new DataException("Invalid class for string type, expecting String or "
                + "CharSequence but found "
                + value.getClass());
          }
          break;
        case BYTES:
          if (value instanceof byte[]) {
            converted = ByteBuffer.wrap((byte[]) value);
          } else if (value instanceof ByteBuffer) {
            converted = value;
          } else if (value instanceof ByteString) {
            converted = ((ByteString) value).asReadOnlyByteBuffer();
          } else {
            throw new DataException("Invalid class for bytes type, expecting byte[], ByteBuffer, "
                + "or ByteString but found "
                + value.getClass());
          }
          break;
        case ARRAY:
          final Schema elemSchema = schema.valueSchema();
          final Collection<Object> array = (Collection<Object>) value;
          final List<Object> newArray = new ArrayList<>(array.size());
          for (Object elem : array) {
            newArray.add(toConnectData(elemSchema, elem));
          }
          converted = newArray;
          break;
        case MAP:
          final Schema keySchema = schema.keySchema();
          final Schema valueSchema = schema.valueSchema();
          final Collection<? extends Message> map = (Collection<? extends Message>) value;
          final Map<Object, Object> newMap = new HashMap<>();
          for (Message message : map) {
            Descriptor descriptor = message.getDescriptorForType();
            Object elemKey = message.getField(descriptor.findFieldByName(KEY_FIELD));
            Object elemValue = message.getField(descriptor.findFieldByName(VALUE_FIELD));
            newMap.put(toConnectData(keySchema, elemKey), toConnectData(valueSchema, elemValue));
          }
          converted = newMap;
          break;
        case STRUCT:
          final Message message = (Message) value;
          final Struct struct = new Struct(schema.schema());
          final Descriptor descriptor = message.getDescriptorForType();

          for (OneofDescriptor oneOfDescriptor : descriptor.getOneofs()) {
            if (message.hasOneof(oneOfDescriptor)) {
              FieldDescriptor fieldDescriptor = message.getOneofFieldDescriptor(oneOfDescriptor);
              Object obj = message.getField(fieldDescriptor);
              if (obj != null) {
                setUnionField(schema, message, struct, oneOfDescriptor, fieldDescriptor);
                break;
              }
            }
          }

          for (FieldDescriptor fieldDescriptor : descriptor.getFields()) {
            OneofDescriptor oneOfDescriptor = fieldDescriptor.getContainingOneof();
            if (oneOfDescriptor != null) {
              // Already added field as oneof
              continue;
            }
            if (fieldDescriptor.getJavaType() != FieldDescriptor.JavaType.MESSAGE
                || fieldDescriptor.isRepeated()
                || message.hasField(fieldDescriptor)) {
              setStructField(schema, message, struct, fieldDescriptor);
            }
          }

          converted = struct;
          break;
        default:
          throw new DataException("Unknown Connect schema type: " + schema.type());
      }

      return converted;
    } catch (ClassCastException e) {
      throw new DataException("Invalid type for " + schema.type() + ": " + value.getClass());
    }
  }

  private void setUnionField(
      Schema schema,
      Message message,
      Struct result,
      OneofDescriptor oneOfDescriptor,
      FieldDescriptor fieldDescriptor
  ) {
    String unionName = oneOfDescriptor.getName() + "_" + oneOfDescriptor.getIndex();
    Field unionField = schema.field(unionName);
    Schema unionSchema = unionField.schema();
    Struct union = new Struct(unionSchema);

    final String fieldName = fieldDescriptor.getName();
    final Field field = unionSchema.field(fieldName);
    Object obj = message.getField(fieldDescriptor);
    union.put(fieldName, toConnectData(field.schema(), obj));

    result.put(unionField, union);
  }

  private void setStructField(
      Schema schema,
      Message message,
      Struct result,
      FieldDescriptor fieldDescriptor
  ) {
    final String fieldName = fieldDescriptor.getName();
    final Field field = schema.field(fieldName);
    Object obj = message.getField(fieldDescriptor);
    result.put(fieldName, toConnectData(field.schema(), obj));
  }

  public Schema toConnectSchema(ProtobufSchema schema) {
    if (schema == null) {
      return null;
    }
    Schema cachedSchema = toConnectSchemaCache.get(schema);
    if (cachedSchema != null) {
      return cachedSchema;
    }
    SchemaBuilder builder = SchemaBuilder.struct();
    Descriptor descriptor = schema.toDescriptor();
    ToConnectContext ctx = new ToConnectContext();
    ctx.put(descriptor.getFullName(), builder);
    Schema resultSchema = toConnectSchema(ctx, builder, descriptor, schema.version()).build();
    toConnectSchemaCache.put(schema, resultSchema);
    return resultSchema;
  }

  private SchemaBuilder toConnectSchema(
      ToConnectContext ctx, SchemaBuilder builder, Descriptor descriptor, Integer version) {
    List<FieldDescriptor> fieldDescriptors = descriptor.getFields();
    if (isMapDescriptor(descriptor, fieldDescriptors)) {
      String name = ProtobufSchema.toMapField(descriptor.getName());
      return SchemaBuilder.map(toConnectSchema(ctx, fieldDescriptors.get(0)),
          toConnectSchema(ctx, fieldDescriptors.get(1))
      )
          .name(name);
    }
    String name = enhancedSchemaSupport ? descriptor.getFullName() : descriptor.getName();
    builder.name(name);
    List<OneofDescriptor> oneOfDescriptors = descriptor.getOneofs();
    for (OneofDescriptor oneOfDescriptor : oneOfDescriptors) {
      String unionName = oneOfDescriptor.getName() + "_" + oneOfDescriptor.getIndex();
      builder.field(unionName, toConnectSchema(ctx, oneOfDescriptor));
    }
    for (FieldDescriptor fieldDescriptor : fieldDescriptors) {
      OneofDescriptor oneOfDescriptor = fieldDescriptor.getContainingOneof();
      if (oneOfDescriptor != null) {
        // Already added field as oneof
        continue;
      }
      builder.field(fieldDescriptor.getName(), toConnectSchema(ctx, fieldDescriptor));
    }

    if (version != null) {
      builder.version(version);
    }

    return builder;
  }

  private Schema toConnectSchema(ToConnectContext ctx, OneofDescriptor descriptor) {
    SchemaBuilder builder = SchemaBuilder.struct();
    builder.name(PROTOBUF_TYPE_UNION_PREFIX + descriptor.getName());
    List<FieldDescriptor> fieldDescriptors = descriptor.getFields();
    for (FieldDescriptor fieldDescriptor : fieldDescriptors) {
      builder.field(fieldDescriptor.getName(), toConnectSchema(ctx, fieldDescriptor));
    }
    builder.optional();
    return builder.build();
  }

  private Schema toConnectSchema(ToConnectContext ctx, FieldDescriptor descriptor) {
    SchemaBuilder builder;

    switch (descriptor.getType()) {
      case INT32:
      case SINT32:
      case SFIXED32: {
        builder = SchemaBuilder.int32();
        break;
      }

      case UINT32:
      case FIXED32:
      case INT64:
      case UINT64:
      case SINT64:
      case FIXED64:
      case SFIXED64: {
        builder = SchemaBuilder.int64();
        break;
      }

      case FLOAT: {
        builder = SchemaBuilder.float32();
        break;
      }

      case DOUBLE: {
        builder = SchemaBuilder.float64();
        break;
      }

      case BOOL: {
        builder = SchemaBuilder.bool();
        break;
      }

      case STRING:
        builder = SchemaBuilder.string();
        break;

      case BYTES:
        builder = SchemaBuilder.bytes();
        break;

      case ENUM:
        builder = SchemaBuilder.string();
        EnumDescriptor enumDescriptor = descriptor.getEnumType();
        builder.name(enumDescriptor.getName());
        builder.parameter(PROTOBUF_TYPE_ENUM, enumDescriptor.getName());
        for (EnumValueDescriptor enumValueDesc : enumDescriptor.getValues()) {
          String enumSymbol = enumValueDesc.getName();
          String enumTag = String.valueOf(enumValueDesc.getNumber());
          builder.parameter(PROTOBUF_TYPE_ENUM_PREFIX + enumSymbol, enumTag);
        }
        break;

      case MESSAGE: {
        if (isTimestampDescriptor(descriptor)) {
          builder = Timestamp.builder();
          break;
        }

        String fullName = descriptor.getMessageType().getFullName();
        builder = ctx.get(fullName);
        if (builder != null) {
          builder = new SchemaWrapper(builder);
        } else {
          builder = SchemaBuilder.struct();
          ctx.put(fullName, builder);
          builder = toConnectSchema(ctx, builder, descriptor.getMessageType(), null);
        }
        break;
      }

      default:
        throw new DataException("Unknown Connect schema type: " + descriptor.getType());
    }

    if (descriptor.isRepeated() && builder.type() != Schema.Type.MAP) {
      Schema schema = builder.optional().build();
      builder = SchemaBuilder.array(schema);
    }

    builder.optional();
    builder.parameter(PROTOBUF_TYPE_TAG, String.valueOf(descriptor.getNumber()));
    return builder.build();
  }

  private static MessageDefinition timestampDefinition() {
    MessageDefinition.Builder timestampType = MessageDefinition.newBuilder(
        GOOGLE_PROTOBUF_TIMESTAMP_NAME);
    timestampType.addField("optional", "int64", "seconds", 1, null);
    timestampType.addField("optional", "int32", "nanos", 2, null);
    return timestampType.build();
  }

  private static boolean isTimestampDescriptor(FieldDescriptor descriptor) {
    String name = descriptor.getMessageType().getFullName();
    return GOOGLE_PROTOBUF_TIMESTAMP_FULL_NAME.equals(name);
  }

  private static boolean isMapDescriptor(
      Descriptor descriptor,
      List<FieldDescriptor> fieldDescriptors
  ) {
    return descriptor.getName().endsWith(MAP_ENTRY_SUFFIX)
        && fieldDescriptors.size() == 2
        && fieldDescriptors.get(0).getName().equals(KEY_FIELD)
        && fieldDescriptors.get(1).getName().equals(VALUE_FIELD);
  }

  /**
   * Split a full dotted-syntax name into a namespace and a single-component name.
   */
  private static String[] splitName(String fullName) {
    String[] result = new String[2];
    int indexLastDot = fullName.lastIndexOf('.');
    if (indexLastDot >= 0) {
      result[0] = fullName.substring(0, indexLastDot);
      result[1] = fullName.substring(indexLastDot + 1);
    } else {
      result[0] = null;
      result[1] = fullName;
    }
    return result;
  }

  /**
   * Strip the namespace from a name.
   */
  private String getUnqualifiedName(String name) {
    String fullName = getNameOrDefault(name);
    int indexLastDot = fullName.lastIndexOf('.');
    if (indexLastDot >= 0) {
      return fullName.substring(indexLastDot + 1);
    } else {
      return fullName;
    }
  }

  private String getNameOrDefault(String name) {
    return name != null && !name.isEmpty()
           ? name
           : DEFAULT_SCHEMA_NAME + (++defaultSchemaNameIndex);
  }

  /**
   * Wraps a SchemaBuilder but overrides the parameters.
   * Parameters are used to specify the field tags.
   */
  static class SchemaWrapper extends SchemaBuilder {

    private final SchemaBuilder builder;
    private final Map<String, String> parameters;

    public SchemaWrapper(SchemaBuilder builder) {
      super(Type.STRUCT);
      this.builder = builder;
      this.parameters = new LinkedHashMap<>();
    }

    @Override
    public boolean isOptional() {
      return builder.isOptional();
    }

    @Override
    public SchemaBuilder optional() {
      return builder.optional();
    }

    @Override
    public SchemaBuilder required() {
      return builder.required();
    }

    @Override
    public Object defaultValue() {
      return builder.defaultValue();
    }

    @Override
    public SchemaBuilder defaultValue(Object value) {
      return builder.defaultValue(value);
    }

    @Override
    public String name() {
      return builder.name();
    }

    @Override
    public SchemaBuilder name(String name) {
      return builder.name(name);
    }

    @Override
    public Integer version() {
      return builder.version();
    }

    @Override
    public SchemaBuilder version(Integer version) {
      return builder.version(version);
    }

    @Override
    public String doc() {
      return builder.doc();
    }

    @Override
    public SchemaBuilder doc(String doc) {
      return builder.doc(doc);
    }

    @Override
    public Map<String, String> parameters() {
      return parameters;
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
      return builder.field(fieldName, fieldSchema);
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
    private final Set<String> structNames;

    public FromConnectContext() {
      this.structNames = new HashSet<>();
    }

    public boolean contains(String structName) {
      return structName != null ? structNames.contains(structName) : false;
    }

    public void add(String structName) {
      if (structName != null) {
        structNames.add(structName);
      }
    }
  }
}
