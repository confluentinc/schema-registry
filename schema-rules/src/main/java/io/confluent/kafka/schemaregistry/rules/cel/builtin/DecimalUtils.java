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

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.confluent.protobuf.type.Decimal;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * Conversion helpers backing {@code decimal(...)} and the {@code decimals.*} operator
 * functions. The CEL surface treats Decimal as the canonical type
 * {@link CelTypeLabels#DECIMAL_NAME}; this client backs it with {@link BigDecimal}.
 */
final class DecimalUtils {

  private DecimalUtils() {
  }

  /**
   * Decode a {@link Decimal} proto message into a {@link BigDecimal}.
   */
  static BigDecimal toBigDecimal(Decimal d) {
    BigInteger unscaled = d.getValue().isEmpty()
        ? BigInteger.ZERO
        : new BigInteger(d.getValue().toByteArray());
    return new BigDecimal(unscaled, d.getScale());
  }

  /**
   * Decode any {@code confluent.type.Decimal} message — concrete generated class or
   * {@link com.google.protobuf.DynamicMessage} produced by a runtime-parsed schema.
   */
  static BigDecimal toBigDecimal(Message msg) {
    FieldDescriptor valueField = msg.getDescriptorForType().findFieldByName("value");
    FieldDescriptor scaleField = msg.getDescriptorForType().findFieldByName("scale");
    ByteString unscaled = (ByteString) msg.getField(valueField);
    int scale = ((Number) msg.getField(scaleField)).intValue();
    BigInteger bi = unscaled.isEmpty()
        ? BigInteger.ZERO
        : new BigInteger(unscaled.toByteArray());
    return new BigDecimal(bi, scale);
  }

  /**
   * Build a {@link BigDecimal} from raw two's-complement big-endian bytes plus scale.
   */
  static BigDecimal toBigDecimal(byte[] bytes, int scale) {
    BigInteger unscaled = bytes.length == 0
        ? BigInteger.ZERO
        : new BigInteger(bytes);
    return new BigDecimal(unscaled, scale);
  }

  /**
   * Runtime dispatch backing {@code decimal(dyn)}. Accepts whatever shape a Proto/Avro
   * decoder commonly produces. Throws {@link IllegalArgumentException} with a hint
   * when the input shape lacks the schema-side metadata needed (raw bytes need a
   * scale; pass it via the two-arg overload).
   */
  static BigDecimal toBigDecimal(Object o) {
    if (o == null) {
      throw new IllegalArgumentException("Cannot convert null to Decimal");
    }
    if (o instanceof BigDecimal) {
      return (BigDecimal) o;
    }
    if (o instanceof Decimal) {
      return toBigDecimal((Decimal) o);
    }
    if (o instanceof Message
        && CelTypeLabels.DECIMAL_NAME.equals(
            ((Message) o).getDescriptorForType().getFullName())) {
      return toBigDecimal((Message) o);
    }
    if (o instanceof Long || o instanceof Integer
        || o instanceof Short || o instanceof Byte) {
      return BigDecimal.valueOf(((Number) o).longValue());
    }
    if (o instanceof Double || o instanceof Float) {
      return BigDecimal.valueOf(((Number) o).doubleValue());
    }
    if (o instanceof String) {
      return new BigDecimal((String) o);
    }
    if (o instanceof byte[] || o instanceof ByteBuffer || o instanceof ByteString) {
      throw new IllegalArgumentException(
          "Cannot convert raw bytes to Decimal without a scale; use "
              + "decimal(bytes, scale) or set useLogicalTypeConverters=true on the "
              + "Avro client so decimal fields arrive as BigDecimal");
    }
    throw new IllegalArgumentException(
        "Cannot convert " + o.getClass().getName() + " to Decimal");
  }
}
