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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
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

    // Logical types: bypass primitive narrowing/coercion ONLY when the value
    // is already the logical-type Java rep (BigDecimal / Temporal / UUID /
    // Variant). The serializer's logical-type Conversion encodes those to the
    // underlying primitive shape. Other values (CelByteString from b"..."
    // literals, raw byte[] / ByteBuffer copied from elsewhere, narrow
    // numerics) fall through to the type-specific case below so Avro receives
    // a recognizable shape (ByteBuffer for BYTES, GenericFixed for FIXED,
    // narrowed primitives for INT/LONG, etc.) — Avro will treat those as the
    // already-encoded form. Unions are handled separately so a logical-typed
    // branch still gets branch-resolved first.
    if (schema.getLogicalType() != null && schema.getType() != Type.UNION) {
      // Null isn't valid for a non-nullable logical-type field — surface the
      // mismatch here rather than letting GenericRecordBuilder.build() fail
      // far away with a less informative message.
      if (celValue == null) {
        throw typeMismatch(null, schema);
      }
      if (isLogicalTypeJavaRep(celValue, schema)) {
        return celValue;
      }
      // Otherwise fall through to type-specific conversion below.
    }

    switch (schema.getType()) {
      case NULL:
        if (celValue != null) {
          throw new AvroTypeException("Expected null for NULL schema, got: " + celValue);
        }
        return null;
      case BOOLEAN:
        if (celValue instanceof Boolean) {
          return celValue;
        }
        throw typeMismatch(celValue, schema);
      case INT:
        return narrowToInt(celValue, schema);
      case LONG:
        return narrowToLong(celValue, schema);
      case FLOAT:
        return narrowToFloat(celValue, schema);
      case DOUBLE:
        return narrowToDouble(celValue, schema);
      case STRING:
        // CharSequence covers String / Utf8 / StringBuilder. EnumSymbol covers
        // chained-transform Maps that route an EnumSymbol value at a STRING-
        // schema field — toString() yields the symbol name, which is the
        // expected Avro string-field representation.
        if (celValue instanceof CharSequence
            || celValue instanceof GenericData.EnumSymbol) {
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
    if (celValue instanceof IndexedRecord) {
      IndexedRecord rec = (IndexedRecord) celValue;
      Schema sourceSchema = rec.getSchema();
      // Same schema → pass through. Schema.equals is structural but Schema
      // caches its hashCode, so identical instances bail at the hash check.
      if (sourceSchema.equals(schema)) {
        return rec;
      }
      // Different schema with the same fullName (or unrelated schema): rebuild
      // against the target. Field-name lookup against the source so schema
      // evolution (added/removed/reordered fields) Just Works — fields present
      // in the target but missing in the source fall back to the target's
      // declared defaults via GenericRecordBuilder.
      GenericRecordBuilder builder = new GenericRecordBuilder(schema);
      for (Field f : schema.getFields()) {
        Field srcField = sourceSchema.getField(f.name());
        if (srcField != null) {
          builder.set(f, convert(rec.get(srcField.pos()), f.schema()));
        }
      }
      return builder.build();
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
    Map<String, Object> out = new LinkedHashMap<>(in.size());
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
    // Pass-through if the caller already constructed a Fixed with the matching
    // schema (mirrors convertRecord's IndexedRecord pass-through).
    // Schema.equals covers name + namespace + size + aliases; the length
    // check defends against malformed Fixed instances whose bytes don't
    // match their declared size. Mismatches fall through to the rebuild
    // path below (unwrapBytes handles GenericFixed).
    if (celValue instanceof GenericData.Fixed) {
      GenericData.Fixed fixed = (GenericData.Fixed) celValue;
      if (fixed.getSchema().equals(schema) && fixed.bytes().length == schema.getFixedSize()) {
        return fixed;
      }
    }
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
    if (celValue instanceof GenericFixed) {
      // Defensive path for Fixed values that didn't go through toCelValue (which
      // would have already wrapped them as CelByteString). Schema-name match is
      // not required here — toFixed handles same-schema pass-through above.
      return ((GenericFixed) celValue).bytes();
    }
    throw typeMismatch(celValue, schema);
  }

  private static GenericData.EnumSymbol toEnumSymbol(Object celValue, Schema schema) {
    // No pass-through: even an EnumSymbol with a matching fullName might carry
    // a symbol the target schema has since removed (schema evolution). Extract
    // the symbol string and validate it against the target before constructing
    // a fresh EnumSymbol bound to the target schema.
    String symbol;
    if (celValue instanceof GenericData.EnumSymbol || celValue instanceof CharSequence) {
      symbol = celValue.toString();
    } else {
      throw typeMismatch(celValue, schema);
    }
    if (!schema.hasEnumSymbol(symbol)) {
      throw new AvroTypeException(
          "Enum " + schema.getFullName() + " has no symbol '" + symbol + "'");
    }
    return new GenericData.EnumSymbol(schema, symbol);
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
        // date / time-millis
        return hasLogicalType && isExpectedTemporalForLogicalType(value, branch);
      case LONG:
        if (value instanceof Long || value instanceof Integer) {
          return true;
        }
        // timestamp-millis / timestamp-micros / time-micros / local-timestamp
        return hasLogicalType && isExpectedTemporalForLogicalType(value, branch);
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
        // CharSequence covers String inputs; EnumSymbol covers values flowing
        // through unchanged from a source record. toEnumSymbol re-validates
        // and rebinds against the target schema regardless of which schema the
        // EnumSymbol originally carried — only the symbol-validity check
        // matters here.
        if (value instanceof CharSequence
            || value instanceof GenericData.EnumSymbol) {
          return branch.hasEnumSymbol(value.toString());
        }
        return false;
      case BYTES:
        if (value instanceof byte[]
            || value instanceof ByteBuffer
            || value instanceof CelByteString
            || value instanceof GenericFixed) {
          return true;
        }
        // decimal: accept BigDecimal for the bytes-backed logical type.
        return hasLogicalType && value instanceof java.math.BigDecimal;
      case FIXED:
        if (value instanceof GenericData.Fixed) {
          GenericData.Fixed fixed = (GenericData.Fixed) value;
          // Schema.equals + length check mirrors toFixed's pass-through
          // criterion. Without the length check a same-fullName-different-
          // size schema would falsely resolve here and then fail in toFixed,
          // potentially preempting a BYTES (or other) branch that would have
          // worked.
          if (fixed.getSchema().equals(branch)
              && fixed.bytes().length == branch.getFixedSize()) {
            return true;
          }
        }
        // Any other GenericFixed (different schema, or wrong-size Fixed that
        // didn't pass strict equality above) — accept by length so toFixed
        // can rebuild against the target schema.
        if (value instanceof GenericFixed) {
          return ((GenericFixed) value).bytes().length == branch.getFixedSize();
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
        // decimal-on-fixed: accept BigDecimal so the logical-type Conversion
        // can encode it. Length isn't checkable here without the scale info,
        // so we trust the Conversion to validate downstream.
        return hasLogicalType && value instanceof java.math.BigDecimal;
      case ARRAY:
        if (value instanceof List) {
          return true;
        }
        // LogicalMap: array<KV> with Map as Java rep — the Conversion encodes
        // the Map into a list of {key, value} records.
        return hasLogicalType && value instanceof Map;
      case MAP:
        return value instanceof Map;
      case RECORD:
        if (value instanceof IndexedRecord) {
          return ((IndexedRecord) value).getSchema().getFullName()
              .equals(branch.getFullName());
        }
        // variant logical-type record: accept Variant so the Conversion can
        // encode it into the {metadata, value} 2-bytes-fields record shape.
        if (hasLogicalType
            && value instanceof io.confluent.kafka.schemaregistry.type.Variant) {
          return true;
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

  /**
   * Recognize the Java types Avro's standard logical-type {@code Conversion}s
   * accept on the write side, gated by the schema's underlying Avro type so a
   * Map for a variant-logical RECORD doesn't bypass record construction, and
   * a Variant for a non-record schema doesn't slip through. Specifically:
   * <ul>
   *   <li>{@link java.math.BigDecimal} — decimal (on bytes or fixed); always
   *       a logical-type rep regardless of underlying Avro type.</li>
   *   <li>{@link java.util.UUID} — uuid (on string); always.</li>
   *   <li>{@link java.time.temporal.Temporal} — date / time-* / timestamp-* /
   *       local-timestamp-* (on int / long); always.</li>
   *   <li>{@link java.util.Map} — Confluent's {@code LogicalMap} logical
   *       type; underlying Avro type is {@code array<KV>}, so gated by
   *       {@code schema.getType() == ARRAY} to avoid bypassing
   *       record/variant conversion.</li>
   *   <li>{@link io.confluent.kafka.schemaregistry.type.Variant} —
   *       Confluent's variant logical type; underlying Avro type is RECORD,
   *       so gated by {@code schema.getType() == RECORD}.</li>
   * </ul>
   * Other shapes (CelByteString, raw byte[], narrow numerics, etc.) fall
   * through to the type-specific case which converts them to the underlying
   * primitive shape Avro expects for the encoded form.
   */
  private static boolean isLogicalTypeJavaRep(Object value, Schema schema) {
    Type t = schema.getType();
    // BigDecimal: decimal logical type — backed by BYTES or FIXED.
    if (value instanceof java.math.BigDecimal) {
      return t == Type.BYTES || t == Type.FIXED;
    }
    // UUID: uuid logical type — backed by STRING.
    if (value instanceof java.util.UUID) {
      return t == Type.STRING;
    }
    // Temporal: date / time-* / timestamp-* / local-timestamp-*
    if (value instanceof java.time.temporal.Temporal) {
      return (t == Type.INT || t == Type.LONG)
          && isExpectedTemporalForLogicalType(value, schema);
    }
    // Map: LogicalMap — backed by array<KV>.
    if (value instanceof java.util.Map) {
      return t == Type.ARRAY;
    }
    // Variant: variant logical type — backed by record.
    if (value instanceof io.confluent.kafka.schemaregistry.type.Variant) {
      return t == Type.RECORD;
    }
    return false;
  }

  private static boolean isExpectedTemporalForLogicalType(Object value, Schema schema) {
    org.apache.avro.LogicalType lt = schema.getLogicalType();
    if (lt == null) {
      return false;
    }
    switch (lt.getName()) {
      case "date":
        return value instanceof java.time.LocalDate;
      case "time-millis":
      case "time-micros":
        return value instanceof java.time.LocalTime;
      case "timestamp-millis":
      case "timestamp-micros":
        return value instanceof java.time.Instant;
      case "local-timestamp-millis":
      case "local-timestamp-micros":
        return value instanceof java.time.LocalDateTime;
      default:
        return false;
    }
  }

  // --- error helpers ------------------------------------------------------

  private static AvroTypeException typeMismatch(Object value, Schema schema) {
    return new AvroTypeException(
        "Cannot convert " + (value == null ? "null" : value.getClass().getName())
            + " to Avro " + schema.getType()
            + (schema.getName() == null ? "" : " (" + schema.getFullName() + ")"));
  }
}
