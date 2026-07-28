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

package io.confluent.kafka.schemaregistry.type.logical.avro;

import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/** Utility class to convert Avro default values to Java data objects. */
public final class AvroDefaultValueConverter {

  private AvroDefaultValueConverter() {}

  /** Converts an Avro default value to a Java data object based on the logical type schema. */
  public static Object toJavaData(final Schema type, final Object defaultValue) {
    switch (type.getType()) {
      case TINYINT:
        if (defaultValue instanceof Number) {
          return ((Number) defaultValue).byteValue();
        }
        break;
      case SMALLINT:
        if (defaultValue instanceof Number) {
          return ((Number) defaultValue).shortValue();
        }
        break;
      case DECIMAL:
        if (defaultValue instanceof byte[]) {
          return new BigDecimal(new BigInteger((byte[]) defaultValue), type.getScale());
        }
        break;
      case DATE:
        if (defaultValue instanceof Integer) {
          return LocalDate.ofEpochDay((int) defaultValue);
        } else if (defaultValue instanceof Long) {
          return LocalDate.ofEpochDay((long) defaultValue);
        }
        break;
      case TIME:
        if (defaultValue instanceof Integer) {
          // time-millis (precision <= 3): millis since midnight
          return LocalTime.ofNanoOfDay(((int) defaultValue) * 1_000_000L);
        } else if (defaultValue instanceof Long) {
          // time-micros (precision 4..9): micros since midnight (data truncated for >6)
          return LocalTime.ofNanoOfDay(((long) defaultValue) * 1_000L);
        }
        break;
      case TIMESTAMP_LTZ:
        if (defaultValue instanceof Long) {
          return toInstant(type.getPrecision(), (long) defaultValue);
        }
        break;
      case TIMESTAMP:
        if (defaultValue instanceof Long) {
          return toLocalDateTime(type.getPrecision(), (long) defaultValue);
        }
        break;
      case ARRAY:
        if (defaultValue instanceof List<?>) {
          return toList(type, (List<?>) defaultValue);
        } else if (defaultValue.getClass().isArray()) {
          return toListFromArray(type, defaultValue);
        }
        break;
      case MAP:
        if (defaultValue instanceof Map) {
          return toMap(type, (Map<?, ?>) defaultValue);
        }
        break;
      case MULTISET:
        if (defaultValue instanceof Map) {
          return toMultisetMap(type, (Map<?, ?>) defaultValue);
        }
        break;
      case ENUM:
        // Symbol name as a String; pass through unchanged.
        return defaultValue;
      case UNION:
        // Avro requires defaults to match the first union branch's type.
        // Recurse to convert using that branch's schema.
        return toJavaData(type.getBranches().get(0).getSchema(), defaultValue);
      default:
        break;
    }
    return defaultValue;
  }

  private static Instant toInstant(final int precision, final long value) {
    if (precision <= 3) {
      return Instant.ofEpochMilli(value);
    } else if (precision <= 6) {
      final long seconds = MICROSECONDS.toSeconds(value);
      long nanoseconds = MICROSECONDS.toNanos(value - SECONDS.toMicros(seconds));
      return Instant.ofEpochSecond(seconds, nanoseconds);
    } else {
      // nanos since epoch
      final long seconds = value / 1_000_000_000L;
      final long nanoseconds = value - seconds * 1_000_000_000L;
      return Instant.ofEpochSecond(seconds, nanoseconds);
    }
  }

  private static LocalDateTime toLocalDateTime(final int precision, final long value) {
    final Instant instant = toInstant(precision, value);
    return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
  }

  private static List<?> toListFromArray(final Schema arrayType, final Object defaultArrayValue) {
    final int length = Array.getLength(defaultArrayValue);
    final List<Object> list = new ArrayList<>(length);
    for (int i = 0; i < length; i++) {
      list.add(toJavaData(arrayType.getElementType(), Array.get(defaultArrayValue, i)));
    }
    return list;
  }

  private static List<?> toList(final Schema arrayType, final List<?> defaultValue) {
    return defaultValue.stream()
        .map(v -> toJavaData(arrayType.getElementType(), v))
        .collect(Collectors.toList());
  }

  private static Map<?, ?> toMap(final Schema mapType, final Map<?, ?> defaultValue) {
    final Map<Object, Object> map = new HashMap<>();
    for (Map.Entry<?, ?> entry : defaultValue.entrySet()) {
      map.put(
          toJavaData(mapType.getKeyType(), entry.getKey()),
          toJavaData(mapType.getValueType(), entry.getValue()));
    }
    return map;
  }

  private static Map<?, ?> toMultisetMap(final Schema multisetType, final Map<?, ?> defaultValue) {
    final Map<Object, Object> map = new HashMap<>();
    for (Map.Entry<?, ?> entry : defaultValue.entrySet()) {
      map.put(
          toJavaData(multisetType.getElementType(), entry.getKey()),
          entry.getValue());
    }
    return map;
  }

  /**
   * Encode a typed Java default value into the Avro-friendly form expected by
   * org.apache.avro.Schema.Field's defaultVal argument.
   */
  public static Object toAvroData(final Schema type, final Object value) {
    if (value == null) {
      return null;
    }
    switch (type.getType()) {
      case TINYINT:
      case SMALLINT:
        if (value instanceof Number) {
          // Avro represents TINYINT/SMALLINT as int32; widen to Integer so
          // JacksonUtils can serialize the default.
          return ((Number) value).intValue();
        }
        return value;
      case BOOLEAN:
      case INT:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case CHAR:
      case VARCHAR:
        return value;
      case BINARY:
      case VARBINARY:
        if (value instanceof byte[]) {
          // Avro encodes bytes defaults as ISO-8859-1 strings (one char per byte).
          return new String((byte[]) value, java.nio.charset.StandardCharsets.ISO_8859_1);
        }
        return value;
      case DECIMAL:
        if (value instanceof BigDecimal) {
          return new String(((BigDecimal) value).unscaledValue().toByteArray(),
              java.nio.charset.StandardCharsets.ISO_8859_1);
        }
        return value;
      case DATE:
        if (value instanceof LocalDate) {
          return (int) ((LocalDate) value).toEpochDay();
        }
        return value;
      case TIME:
        if (value instanceof LocalTime) {
          int precision = type.getPrecision();
          if (precision <= 3) {
            // time-millis: int millis since midnight
            return (int) (((LocalTime) value).toNanoOfDay() / 1_000_000L);
          }
          // time-micros: long micros since midnight (sub-µs truncated for >6)
          return ((LocalTime) value).toNanoOfDay() / 1_000L;
        }
        return value;
      case TIMESTAMP:
        if (value instanceof LocalDateTime) {
          return toEpochUnits(type.getPrecision(),
              ((LocalDateTime) value).toInstant(ZoneOffset.UTC));
        }
        return value;
      case TIMESTAMP_LTZ:
        if (value instanceof Instant) {
          return toEpochUnits(type.getPrecision(), (Instant) value);
        }
        return value;
      case ENUM:
        // Symbol name as a String; Avro emits enum defaults as strings.
        return value;
      case UNION:
        // Avro requires defaults to match the first union branch's type.
        // Encode using that branch's schema converter.
        return toAvroData(type.getBranches().get(0).getSchema(), value);
      default:
        throw new ValidationException(
            "Default values are not supported for type: " + type.getType());
    }
  }

  private static long toEpochUnits(final int precision, final Instant instant) {
    if (precision <= 3) {
      return instant.toEpochMilli();
    } else if (precision <= 6) {
      return SECONDS.toMicros(instant.getEpochSecond())
          + instant.getNano() / 1000L;
    }
    return instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
  }
}
