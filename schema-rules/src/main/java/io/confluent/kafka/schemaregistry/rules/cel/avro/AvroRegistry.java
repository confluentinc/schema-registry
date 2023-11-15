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

import static org.projectnessie.cel.common.types.Err.newErr;

import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.util.Utf8;
import org.projectnessie.cel.common.types.StringT;
import org.projectnessie.cel.common.types.ref.FieldType;
import org.projectnessie.cel.common.types.ref.Type;
import org.projectnessie.cel.common.types.ref.TypeAdapterSupport;
import org.projectnessie.cel.common.types.ref.TypeRegistry;
import org.projectnessie.cel.common.types.ref.Val;

/**
 * CEL-Java {@link TypeRegistry} to use Avro objects as input values for CEL scripts.
 *
 * <p>The implementation does not support the construction of Avro objects in CEL expressions and
 * therefore returning Avro objects from CEL expressions is not possible/implemented and results
 * in {@link UnsupportedOperationException}s.
 */
public final class AvroRegistry implements TypeRegistry {
  private final Map<Schema, AvroTypeDescription> knownTypes = new HashMap<>();
  private final Map<String, AvroTypeDescription> knownTypesByName = new HashMap<>();

  private final Map<Schema, AvroEnumDescription> enumMap = new HashMap<>();
  private final Map<String, AvroEnumValue> enumValues = new HashMap<>();

  private AvroRegistry() {
  }

  public static TypeRegistry newRegistry() {
    return new AvroRegistry();
  }

  @Override
  public TypeRegistry copy() {
    return this;
  }

  @Override
  public void register(Object t) {
    Schema s;
    if (t instanceof Schema) {
      s = (Schema) t;
    } else if (t instanceof GenericContainer) {
      s = ((GenericContainer)t).getSchema();
    } else {
      throw new IllegalArgumentException("argument is not of type Schema or GenericContainer");
    }
    if (s.getType() == Schema.Type.RECORD) {
      typeDescription(s);
    }
  }

  @Override
  public void registerType(Type... types) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Val enumValue(String enumName) {
    AvroEnumValue enumVal = enumValues.get(enumName);
    if (enumVal == null) {
      return newErr("unknown enum name '%s'", enumName);
    }
    return enumVal.stringValue();
  }

  @Override
  public Val findIdent(String identName) {
    AvroTypeDescription td = knownTypesByName.get(identName);
    if (td != null) {
      return td.type();
    }

    AvroEnumValue enumVal = enumValues.get(identName);
    if (enumVal != null) {
      return enumVal.stringValue();
    }
    return null;
  }

  @Override
  public com.google.api.expr.v1alpha1.Type findType(String typeName) {
    AvroTypeDescription td = knownTypesByName.get(typeName);
    if (td == null) {
      return null;
    }
    return td.pbType();
  }

  @Override
  public FieldType findFieldType(String messageType, String fieldName) {
    AvroTypeDescription td = knownTypesByName.get(messageType);
    if (td == null) {
      return null;
    }
    return td.fieldType(fieldName);
  }

  @Override
  public Val newValue(String typeName, Map<String, Val> fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Val nativeToValue(Object value) {
    if (value instanceof Val) {
      return (Val) value;
    }
    Val maybe = TypeAdapterSupport.maybeNativeToValue(this, value);
    if (maybe != null) {
      return maybe;
    }

    if (value instanceof Utf8) {
      return StringT.stringOf(value.toString());
    }

    if (value instanceof GenericEnumSymbol) {
      String fq = AvroEnumValue.fullyQualifiedName(((GenericEnumSymbol<?>) value));
      AvroEnumValue v = enumValues.get(fq);
      if (v == null) {
        return newErr("unknown enum name '%s'", fq);
      }
      return v.stringValue();
    }

    try {
      return AvroObjectT.newObject(
          this, value, typeDescription(((GenericContainer) value).getSchema()));
    } catch (Exception e) {
      throw new RuntimeException("oops", e);
    }
  }

  AvroEnumDescription enumDescription(Schema schema) {
    if (schema.getType() != Schema.Type.ENUM) {
      throw new IllegalArgumentException("only enum allowed here");
    }

    AvroEnumDescription ed = enumMap.get(schema);
    if (ed != null) {
      return ed;
    }
    ed = computeEnumDescription(schema);
    enumMap.put(schema, ed);
    return ed;
  }

  private AvroEnumDescription computeEnumDescription(Schema schema) {
    AvroEnumDescription enumDesc = new AvroEnumDescription(schema);
    enumMap.put(schema, enumDesc);

    enumDesc.buildValues().forEach(v -> enumValues.put(v.fullyQualifiedName(), v));

    return enumDesc;
  }

  AvroTypeDescription typeDescription(Schema schema) {
    if (schema.getType() == Schema.Type.ENUM) {
      throw new IllegalArgumentException("enum not allowed here");
    }

    AvroTypeDescription td = knownTypes.get(schema);
    if (td != null) {
      return td;
    }
    td = computeTypeDescription(schema);
    knownTypes.put(schema, td);
    return td;
  }

  private AvroTypeDescription computeTypeDescription(Schema schema) {
    AvroTypeDescription typeDesc = new AvroTypeDescription(schema, this::typeQuery);
    knownTypesByName.put(schema.getFullName(), typeDesc);

    return typeDesc;
  }

  private com.google.api.expr.v1alpha1.Type typeQuery(Schema schema) {
    if (schema.getType() == Schema.Type.ENUM) {
      return enumDescription(schema).pbType();
    }
    return typeDescription(schema).pbType();
  }
}
