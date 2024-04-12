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

import static org.projectnessie.cel.common.types.Err.newTypeConversionError;
import static org.projectnessie.cel.common.types.Err.noSuchField;
import static org.projectnessie.cel.common.types.Err.noSuchOverload;
import static org.projectnessie.cel.common.types.Types.boolOf;

import org.projectnessie.cel.common.types.ObjectT;
import org.projectnessie.cel.common.types.StringT;
import org.projectnessie.cel.common.types.ref.Val;

public final class AvroObjectT extends ObjectT {

  private AvroObjectT(AvroRegistry registry, Object value, AvroTypeDescription typeDesc) {
    super(registry, value, typeDesc, typeDesc.type());
  }

  static AvroObjectT newObject(
      AvroRegistry registry, Object value, AvroTypeDescription typeDesc) {
    return new AvroObjectT(registry, value, typeDesc);
  }

  AvroTypeDescription typeDesc() {
    return (AvroTypeDescription) typeDesc;
  }

  AvroRegistry registry() {
    return (AvroRegistry) adapter;
  }

  @Override
  public Val isSet(Val field) {
    if (!(field instanceof StringT)) {
      return noSuchOverload(this, "isSet", field);
    }
    String fieldName = (String) field.value();

    if (!typeDesc().hasProperty(fieldName)) {
      return noSuchField(fieldName);
    }

    Object value = typeDesc().fromObject(value(), fieldName);

    return boolOf(value != null);
  }

  @Override
  public Val get(Val index) {
    if (!(index instanceof StringT)) {
      return noSuchOverload(this, "get", index);
    }
    String fieldName = (String) index.value();

    if (!typeDesc().hasProperty(fieldName)) {
      return noSuchField(fieldName);
    }

    Object v = typeDesc().fromObject(value(), fieldName);

    return registry().nativeToValue(v);
  }

  @Override
  public <T> T convertToNative(Class<T> typeDesc) {
    if (typeDesc.isAssignableFrom(value.getClass())) {
      return (T) value;
    }
    if (typeDesc.isAssignableFrom(getClass())) {
      return (T) this;
    }
    throw new IllegalArgumentException(
        newTypeConversionError(value.getClass().getName(), typeDesc).toString());
  }
}
