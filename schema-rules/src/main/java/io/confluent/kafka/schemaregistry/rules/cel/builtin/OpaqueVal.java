/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rules.cel.builtin;

import java.util.Objects;
import org.projectnessie.cel.common.types.Err;
import org.projectnessie.cel.common.types.TypeT;
import org.projectnessie.cel.common.types.Types;
import org.projectnessie.cel.common.types.ref.BaseVal;
import org.projectnessie.cel.common.types.ref.Type;
import org.projectnessie.cel.common.types.ref.Val;

/**
 * A {@link Val} that wraps a native Java object as an opaque CEL object type. Used for
 * the extended types (Decimal, Variant) that have no CEL-native representation. The
 * function bindings unwrap via {@link #value()} and re-wrap via {@code OpaqueVal.of(...)}.
 */
public final class OpaqueVal extends BaseVal {

  private final Type type;
  private final Object value;

  private OpaqueVal(Type type, Object value) {
    this.type = type;
    this.value = value;
  }

  public static OpaqueVal of(String typeName, Object value) {
    return new OpaqueVal(TypeT.newObjectTypeValue(typeName), value);
  }

  @Override
  public Type type() {
    return type;
  }

  @Override
  public Object value() {
    return value;
  }

  @Override
  public Val equal(Val other) {
    if (!(other instanceof OpaqueVal)) {
      return Err.noSuchOverload(this, "equal", other);
    }
    OpaqueVal that = (OpaqueVal) other;
    return Types.boolOf(
        Objects.equals(this.type.typeName(), that.type.typeName())
            && Objects.equals(this.value, that.value));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T convertToNative(Class<T> typeDesc) {
    if (typeDesc.isInstance(value)) {
      return (T) value;
    }
    throw new UnsupportedOperationException(
        "Cannot convert " + type.typeName() + " to " + typeDesc.getName());
  }

  @Override
  public Val convertToType(Type targetType) {
    if (Objects.equals(this.type.typeName(), targetType.typeName())) {
      return this;
    }
    return Err.newTypeConversionError(this.type, targetType);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OpaqueVal)) {
      return false;
    }
    OpaqueVal that = (OpaqueVal) o;
    return Objects.equals(type.typeName(), that.type.typeName())
        && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type.typeName(), value);
  }

  @Override
  public String toString() {
    return type.typeName() + "(" + value + ")";
  }
}
