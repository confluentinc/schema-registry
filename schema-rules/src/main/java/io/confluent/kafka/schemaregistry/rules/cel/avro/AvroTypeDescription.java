/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rules.cel.avro;

import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.projectnessie.cel.common.types.TypeT;
import org.projectnessie.cel.common.types.pb.Checked;
import org.projectnessie.cel.common.types.ref.FieldType;
import org.projectnessie.cel.common.types.ref.Type;
import org.projectnessie.cel.common.types.ref.TypeDescription;

public final class AvroTypeDescription implements TypeDescription {

  public static final Schema NULL_AVRO_SCHEMA = Schema.create(Schema.Type.NULL);

  private final Schema schema;
  private final String fullName;
  private final Type type;
  private final com.google.api.expr.v1alpha1.Type pbType;

  private final Map<String, AvroFieldType> fieldTypes;

  AvroTypeDescription(Schema schema, TypeQuery typeQuery) {
    this.schema = schema;
    this.fullName = schema.getFullName();
    this.type = TypeT.newObjectTypeValue(fullName);
    this.pbType = com.google.api.expr.v1alpha1.Type.newBuilder().setMessageType(fullName).build();

    fieldTypes = new HashMap<>();

    for (Schema.Field field : schema.getFields()) {
      String n = field.name();

      AvroFieldType ft =
          new AvroFieldType(
              findTypeForAvroType(field.schema(), typeQuery),
              target -> fromObject(target, n) != null,
              target -> fromObject(target, n),
              field.schema());
      fieldTypes.put(n, ft);
    }
  }

  @FunctionalInterface
  interface TypeQuery {
    com.google.api.expr.v1alpha1.Type getType(Schema schema);
  }

  com.google.api.expr.v1alpha1.Type findTypeForAvroType(Schema schema, TypeQuery typeQuery) {
    Schema.Type type = schema.getType();
    switch (type) {
      case BOOLEAN:
        return Checked.checkedBool;
      case INT:
      case LONG:
        return Checked.checkedInt;
      case BYTES:
      case FIXED:
        return Checked.checkedBytes;
      case FLOAT:
      case DOUBLE:
        return Checked.checkedDouble;
      case STRING:
        return Checked.checkedString;
      // TODO duration, timestamp
      case ARRAY:
        return Checked.checkedListDyn;
      case MAP:
        return Checked.checkedMapStringDyn;
      case ENUM:
        return typeQuery.getType(schema);
      case NULL:
        return Checked.checkedNull;
      case RECORD:
        return typeQuery.getType(schema);
      case UNION:
        if (schema.getTypes().size() == 2 && schema.getTypes().contains(NULL_AVRO_SCHEMA)) {
          for (Schema memberSchema : schema.getTypes()) {
            if (!memberSchema.equals(NULL_AVRO_SCHEMA)) {
              return findTypeForAvroType(memberSchema, typeQuery);
            }
          }
        }
        throw new IllegalArgumentException("Unsupported union type");
      default:
        throw new IllegalArgumentException("Unsupported type " + type);
    }
  }

  boolean hasProperty(String property) {
    return fieldTypes.containsKey(property);
  }

  Object fromObject(Object value, String property) {
    AvroFieldType ft = fieldTypes.get(property);
    if (ft == null) {
      throw new IllegalArgumentException(String.format("No property named '%s'", property));
    }
    Schema schema = getSchema(value);
    GenericData data = getData(value);
    Schema.Field f = schema.getField(property);
    Object result = data.getField(value, f.name(), f.pos());
    if (result instanceof Utf8) {
      result = result.toString();
    }
    return result;
  }

  Type type() {
    return type;
  }

  com.google.api.expr.v1alpha1.Type pbType() {
    return pbType;
  }

  FieldType fieldType(String fieldName) {
    return fieldTypes.get(fieldName);
  }

  @Override
  public String name() {
    return fullName;
  }

  @Override
  public Class<?> reflectType() {
    try {
      return Class.forName(fullName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  static Schema getSchema(Object message) {
    if (!(message instanceof GenericContainer)) {
      throw new IllegalArgumentException("Object is not a GenericContainer");
    }
    return ((GenericContainer) message).getSchema();
  }

  static GenericData getData(Object message) {
    if (message instanceof SpecificRecord) {
      return SpecificData.get();
    } else if (message instanceof GenericRecord) {
      return GenericData.get();
    } else {
      return  ReflectData.get();
    }
  }

}
