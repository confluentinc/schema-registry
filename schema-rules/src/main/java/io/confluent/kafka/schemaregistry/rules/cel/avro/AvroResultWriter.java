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

package io.confluent.kafka.schemaregistry.rules.cel.avro;

import dev.cel.common.values.CelByteString;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;

/**
 * Converts a CEL evaluation result (typically a Map literal or scalar) back into
 * the Avro representation matching a target {@link Schema}. Constructs
 * {@link org.apache.avro.generic.GenericRecord}s field-by-field via
 * {@link GenericRecordBuilder} so we never roundtrip through JSON — that
 * roundtrip lossily encodes bytes as base64, requires {@code {"branchName":...}}
 * tagging for unions, and can't narrow CEL's widened {@code int64} back to an
 * {@code int} field.
 *
 * <p>Used only by the Avro path of {@code CelExecutor#transform}; JSON Schema
 * and Protobuf targets keep their existing conversion paths.
 */
public final class AvroResultWriter {

  private AvroResultWriter() {
  }

  /**
   * Convert {@code celValue} into the Avro shape required by {@code schema}.
   * Recursively walks records, unions, arrays, and maps. For logical types,
   * delegates to Avro's own {@code Conversion} infrastructure by passing the
   * value through unchanged — the caller's {@link GenericRecordBuilder} (or the
   * downstream serializer's {@code GenericData}) handles {@code Instant} ↔
   * {@code Long}, {@code BigDecimal} ↔ bytes+scale, etc.
   */
  public static Object convert(Object celValue, Schema schema) {
    // Normalize CEL's null representations (dev.cel.common.values.NullValue,
    // com.google.protobuf.NullValue) to Java null up front so the rest of the
    // walker can use a single null check.
    if (isCelNull(celValue)) {
      celValue = null;
    }

    // Logical types: bypass primitive narrowing/coercion. The serializer's
    // logical-type Conversion handles round-tripping. Unions are handled
    // separately so a logical-type union still gets branch-resolved.
    if (schema.getLogicalType() != null && schema.getType() != Type.UNION) {
      // Null isn't valid for a non-nullable logical-type field — surface the
      // mismatch here rather than letting GenericRecordBuilder.build() fail
      // far away with a less informative message.
      if (celValue == null) {
        throw typeMismatch(null, schema);
      }
      return celValue;
    }

    switch (schema.getType()) {
      case NULL:
        if (celValue != null) {
          throw new AvroTypeException("Expected null for NULL schema, got: " + celValue);
        }
        return null;
      case BOOLEAN:
        return requireType(celValue, Boolean.class, schema);
      case INT:
        return narrowToInt(celValue, schema);
      case LONG:
        return narrowToLong(celValue, schema);
      case FLOAT:
        return narrowToFloat(celValue, schema);
      case DOUBLE:
        return narrowToDouble(celValue, schema);
      case STRING:
        if (celValue instanceof CharSequence) {
          return celValue.toString();
        }
        throw typeMismatch(celValue, schema);
      case BYTES:
        return toByteBuffer(celValue, schema);
      case FIXED:
        return toFixed(celValue, schema);
      case ENUM:
        return toEnumSymbol(celValue, schema);
      case ARRAY:
        return convertArray(celValue, schema);
      case MAP:
        return convertMap(celValue, schema);
      case RECORD:
        return convertRecord(celValue, schema);
      case UNION:
        return convertUnion(celValue, schema);
      default:
        throw new AvroTypeException("Unsupported Avro type: " + schema.getType());
    }
  }

  // --- record / union / collection ----------------------------------------

  private static Object convertRecord(Object celValue, Schema schema) {
    if (celValue == null) {
      throw typeMismatch(null, schema);
    }
    // Pass-through if a caller already built a record with the matching schema.
    if (celValue instanceof IndexedRecord) {
      IndexedRecord rec = (IndexedRecord) celValue;
      if (rec.getSchema().getFullName().equals(schema.getFullName())) {
        return rec;
      }
    }
    if (!(celValue instanceof Map)) {
      throw typeMismatch(celValue, schema);
    }
    Map<?, ?> map = (Map<?, ?>) celValue;
    GenericRecordBuilder builder = new GenericRecordBuilder(schema);
    for (Field f : schema.getFields()) {
      // Missing keys: leave unset and let GenericRecordBuilder.build() apply
      // the field's declared default (or throw AvroRuntimeException if none).
      if (!map.containsKey(f.name())) {
        continue;
      }
      Object raw = map.get(f.name());
      builder.set(f, convert(raw, f.schema()));
    }
    return builder.build();
  }

  private static Object convertUnion(Object celValue, Schema schema) {
    int idx = resolveUnion(schema, celValue);
    return convert(celValue, schema.getTypes().get(idx));
  }

  private static Object convertArray(Object celValue, Schema schema) {
    if (!(celValue instanceof List)) {
      throw typeMismatch(celValue, schema);
    }
    List<?> in = (List<?>) celValue;
    List<Object> out = new ArrayList<>(in.size());
    for (Object e : in) {
      out.add(convert(e, schema.getElementType()));
    }
    return out;
  }

  private static Object convertMap(Object celValue, Schema schema) {
    if (!(celValue instanceof Map)) {
      throw typeMismatch(celValue, schema);
    }
    Map<?, ?> in = (Map<?, ?>) celValue;
    java.util.Map<String, Object> out = new java.util.LinkedHashMap<>(in.size());
    for (Map.Entry<?, ?> e : in.entrySet()) {
      out.put(String.valueOf(e.getKey()), convert(e.getValue(), schema.getValueType()));
    }
    return out;
  }

  // --- bytes / fixed / enum -----------------------------------------------

  private static ByteBuffer toByteBuffer(Object celValue, Schema schema) {
    if (celValue instanceof ByteBuffer) {
      // Defensive duplicate so position-mutating reads downstream don't leak
      // back to whatever value the user's CEL expression is referencing.
      return ((ByteBuffer) celValue).duplicate();
    }
    byte[] bytes = unwrapBytes(celValue, schema);
    return ByteBuffer.wrap(bytes);
  }

  private static GenericData.Fixed toFixed(Object celValue, Schema schema) {
    byte[] bytes = unwrapBytes(celValue, schema);
    if (bytes.length != schema.getFixedSize()) {
      throw new AvroTypeException(
          "Fixed schema " + schema.getFullName() + " expects " + schema.getFixedSize()
              + " bytes, got " + bytes.length);
    }
    return new GenericData.Fixed(schema, bytes);
  }

  private static byte[] unwrapBytes(Object celValue, Schema schema) {
    if (celValue instanceof CelByteString) {
      return ((CelByteString) celValue).toByteArray();
    }
    if (celValue instanceof byte[]) {
      return (byte[]) celValue;
    }
    if (celValue instanceof ByteBuffer) {
      ByteBuffer buf = ((ByteBuffer) celValue).duplicate();
      byte[] out = new byte[buf.remaining()];
      buf.get(out);
      return out;
    }
    throw typeMismatch(celValue, schema);
  }

  private static GenericData.EnumSymbol toEnumSymbol(Object celValue, Schema schema) {
    if (celValue instanceof GenericData.EnumSymbol
        && ((GenericData.EnumSymbol) celValue).getSchema().getFullName()
            .equals(schema.getFullName())) {
      return (GenericData.EnumSymbol) celValue;
    }
    if (celValue instanceof CharSequence) {
      String symbol = celValue.toString();
      if (!schema.hasEnumSymbol(symbol)) {
        throw new AvroTypeException(
            "Enum " + schema.getFullName() + " has no symbol '" + symbol + "'");
      }
      return new GenericData.EnumSymbol(schema, symbol);
    }
    throw typeMismatch(celValue, schema);
  }

  // --- numeric narrowing --------------------------------------------------

  private static Integer narrowToInt(Object celValue, Schema schema) {
    if (celValue instanceof Integer) {
      return (Integer) celValue;
    }
    if (celValue instanceof Long) {
      long l = (Long) celValue;
      if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
        throw new AvroTypeException(
            "Value " + l + " out of range for INT field");
      }
      return (int) l;
    }
    throw typeMismatch(celValue, schema);
  }

  private static Long narrowToLong(Object celValue, Schema schema) {
    if (celValue instanceof Long) {
      return (Long) celValue;
    }
    if (celValue instanceof Integer) {
      return ((Integer) celValue).longValue();
    }
    throw typeMismatch(celValue, schema);
  }

  private static Float narrowToFloat(Object celValue, Schema schema) {
    if (celValue instanceof Float) {
      return (Float) celValue;
    }
    if (celValue instanceof Number) {
      // Number.floatValue() handles Double too, with the implicit precision
      // loss the user accepts by declaring a float field.
      return ((Number) celValue).floatValue();
    }
    throw typeMismatch(celValue, schema);
  }

  private static Double narrowToDouble(Object celValue, Schema schema) {
    if (celValue instanceof Double) {
      return (Double) celValue;
    }
    if (celValue instanceof Number) {
      return ((Number) celValue).doubleValue();
    }
    throw typeMismatch(celValue, schema);
  }

  // --- union resolution ---------------------------------------------------

  /**
   * CEL-aware union resolver. Picks the first declaration-order branch whose
   * Avro type accepts the CEL value's Java shape. More permissive than
   * {@link GenericData#resolveUnion} because CEL widens {@code int → int64}
   * and represents records as plain {@link Map}s — neither of which the
   * default resolver matches against an {@code int} or record branch.
   */
  private static int resolveUnion(Schema unionSchema, Object value) {
    List<Schema> branches = unionSchema.getTypes();
    for (int i = 0; i < branches.size(); i++) {
      if (branchAccepts(branches.get(i), value)) {
        return i;
      }
    }
    throw new UnresolvedUnionException(unionSchema, value);
  }

  private static boolean branchAccepts(Schema branch, Object value) {
    if (value == null) {
      return branch.getType() == Type.NULL;
    }
    boolean hasLogicalType = branch.getLogicalType() != null;
    switch (branch.getType()) {
      case NULL:
        return false;
      case BOOLEAN:
        return value instanceof Boolean;
      case INT:
        if (value instanceof Integer) {
          return true;
        }
        if (value instanceof Long) {
          long l = (Long) value;
          return l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE;
        }
        // date / time-millis: pass through Temporal so the logical-type
        // Conversion handles LocalDate/LocalTime ↔ int.
        return hasLogicalType && value instanceof java.time.temporal.Temporal;
      case LONG:
        if (value instanceof Long || value instanceof Integer) {
          return true;
        }
        // timestamp-millis / timestamp-micros / time-micros / local-timestamp:
        // accept java.time.* types so they match this branch.
        return hasLogicalType && value instanceof java.time.temporal.Temporal;
      case FLOAT:
      case DOUBLE:
        return value instanceof Number;
      case STRING:
        if (value instanceof CharSequence) {
          return true;
        }
        // uuid: accept java.util.UUID for the string-backed logical type.
        return hasLogicalType && value instanceof java.util.UUID;
      case ENUM:
        return value instanceof CharSequence
            && branch.hasEnumSymbol(value.toString());
      case BYTES:
        if (value instanceof byte[]
            || value instanceof ByteBuffer
            || value instanceof CelByteString) {
          return true;
        }
        // decimal: accept BigDecimal for the bytes-backed logical type.
        return hasLogicalType && value instanceof java.math.BigDecimal;
      case FIXED:
        if (value instanceof GenericData.Fixed) {
          return ((GenericData.Fixed) value).getSchema().getFullName()
              .equals(branch.getFullName());
        }
        if (value instanceof byte[]) {
          return ((byte[]) value).length == branch.getFixedSize();
        }
        if (value instanceof ByteBuffer) {
          return ((ByteBuffer) value).remaining() == branch.getFixedSize();
        }
        if (value instanceof CelByteString) {
          return ((CelByteString) value).size() == branch.getFixedSize();
        }
        return false;
      case ARRAY:
        return value instanceof List;
      case MAP:
        return value instanceof Map;
      case RECORD:
        if (value instanceof IndexedRecord) {
          return ((IndexedRecord) value).getSchema().getFullName()
              .equals(branch.getFullName());
        }
        // CEL outputs records as Maps. Without explicit tagging we accept any
        // Map for any record branch; first record branch in declaration order
        // wins (per the agreed convention). Authors who need disambiguation
        // can put a more specific non-record branch first or pass an
        // IndexedRecord directly.
        return value instanceof Map;
      case UNION:
        // Avro disallows nested unions, but defend anyway.
        return false;
      default:
        return false;
    }
  }

  // --- null normalization -------------------------------------------------

  /**
   * CEL's {@code null} literal evaluates to {@code dev.cel.common.values.NullValue}
   * (Google cel-java) rather than Java {@code null}. Some bindings can also hand
   * us {@link com.google.protobuf.NullValue}. Treat both as Avro null.
   */
  private static boolean isCelNull(Object value) {
    return value instanceof dev.cel.common.values.NullValue
        || value instanceof com.google.protobuf.NullValue;
  }

  // --- error helpers ------------------------------------------------------

  private static <T> T requireType(Object value, Class<T> type, Schema schema) {
    if (type.isInstance(value)) {
      return type.cast(value);
    }
    throw typeMismatch(value, schema);
  }

  private static AvroTypeException typeMismatch(Object value, Schema schema) {
    return new AvroTypeException(
        "Cannot convert " + (value == null ? "null" : value.getClass().getName())
            + " to Avro " + schema.getType()
            + (schema.getName() == null ? "" : " (" + schema.getFullName() + ")"));
  }
}
