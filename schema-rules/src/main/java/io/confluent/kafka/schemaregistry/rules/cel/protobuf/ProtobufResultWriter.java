/*
 * Copyright 2025 Confluent Inc.
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
 */

package io.confluent.kafka.schemaregistry.rules.cel.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import io.confluent.kafka.schemaregistry.rules.cel.builtin.CelDecimal;
import io.confluent.kafka.schemaregistry.type.Variant;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Re-shapes a CEL result so that Jackson renders it as protobuf JSON, the counterpart of
 * {@code AvroResultWriter} on the Avro path.
 *
 * <p>A message-level CEL rule returns a map of field name to value, which
 * {@code CelExecutor} serializes with Jackson and hands to
 * {@code ProtobufSchema.fromJson}. Two CEL values have no protobuf JSON rendering of their
 * own: a decimal is a {@code confluent.type.Decimal} <em>message</em>, not a number, and a
 * {@link Variant} is a {@code confluent.type.Variant} message rather than a POJO Jackson can
 * reflect over. Left alone the first fails with "Expect message object but got: 13.34" and the
 * second throws out of {@code Variant.valueBuffer}.
 *
 * <p>The walk is driven by the target descriptor rather than by the value's Java type, so a
 * decimal produced for a plain numeric field still renders as a number — only a field actually
 * declared as one of these message types is re-shaped. Bytes are emitted as {@code byte[]},
 * which Jackson base64-encodes, and base64 is protobuf JSON's encoding for {@code bytes}.
 */
public final class ProtobufResultWriter {

  public static final String DECIMAL_TYPE_NAME = "confluent.type.Decimal";
  public static final String VARIANT_TYPE_NAME = "confluent.type.Variant";

  private ProtobufResultWriter() {
  }

  /**
   * Converts {@code result} against {@code descriptor}. Anything the descriptor does not
   * describe is returned untouched, so this is safe to call on any CEL result.
   */
  public static Object convert(Object result, Descriptor descriptor) {
    if (!(result instanceof Map) || descriptor == null) {
      return result;
    }
    Map<?, ?> in = (Map<?, ?>) result;
    Map<Object, Object> out = new LinkedHashMap<>(in.size());
    for (Map.Entry<?, ?> e : in.entrySet()) {
      Object key = e.getKey();
      FieldDescriptor fd = key == null ? null : descriptor.findFieldByName(String.valueOf(key));
      out.put(key, fd == null ? e.getValue() : convertField(fd, e.getValue()));
    }
    return out;
  }

  private static Object convertField(FieldDescriptor fd, Object value) {
    if (value == null || fd.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
      return value;
    }
    if (fd.isRepeated() && value instanceof List) {
      List<?> in = (List<?>) value;
      List<Object> out = new ArrayList<>(in.size());
      for (Object element : in) {
        out.add(convertMessage(fd.getMessageType(), element));
      }
      return out;
    }
    return convertMessage(fd.getMessageType(), value);
  }

  private static Object convertMessage(Descriptor desc, Object value) {
    String name = desc.getFullName();
    if (DECIMAL_TYPE_NAME.equals(name)) {
      BigDecimal dec = asBigDecimal(value);
      if (dec != null) {
        // Same mapping as DecimalUtils.fromBigDecimal: precision and scale describe the value.
        Map<String, Object> m = new LinkedHashMap<>(3);
        m.put("value", dec.unscaledValue().toByteArray());
        m.put("precision", dec.precision());
        m.put("scale", dec.scale());
        return m;
      }
      return value;
    }
    if (VARIANT_TYPE_NAME.equals(name) && value instanceof Variant) {
      Variant variant = (Variant) value;
      Map<String, Object> m = new LinkedHashMap<>(2);
      m.put("metadata", toBytes(variant.getMetadataBuffer()));
      m.put("value", toBytes(variant.getValueBuffer()));
      return m;
    }
    // A nested message: keep descending so a decimal or variant further down is found too.
    return convert(value, desc);
  }

  private static BigDecimal asBigDecimal(Object value) {
    if (value instanceof CelDecimal) {
      return ((CelDecimal) value).value();
    }
    if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    }
    return null;
  }

  private static byte[] toBytes(ByteBuffer buffer) {
    ByteBuffer dup = buffer.duplicate();
    byte[] out = new byte[dup.remaining()];
    dup.get(out);
    return out;
  }
}
