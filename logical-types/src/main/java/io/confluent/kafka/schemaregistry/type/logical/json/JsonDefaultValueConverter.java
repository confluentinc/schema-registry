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

package io.confluent.kafka.schemaregistry.type.logical.json;

import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.json.JSONArray;
import org.json.JSONObject;

/** Encodes/decodes default values for JSON Schema. */
public final class JsonDefaultValueConverter {

  private JsonDefaultValueConverter() {}

  /** Encode a typed Java default value into a JSON-schema-friendly form. */
  public static Object toJsonValue(final Schema type, final Object value) {
    if (value == null) {
      return null;
    }
    switch (type.getType()) {
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
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
          return Base64.getEncoder().encodeToString((byte[]) value);
        }
        return value;
      case DECIMAL:
        if (value instanceof BigDecimal) {
          return value.toString();
        }
        return value;
      case DATE:
        if (value instanceof LocalDate) {
          return value.toString();
        }
        return value;
      case TIME:
        if (value instanceof LocalTime) {
          return value.toString();
        }
        return value;
      case TIMESTAMP:
        if (value instanceof LocalDateTime) {
          return value.toString();
        }
        return value;
      case TIMESTAMP_LTZ:
        if (value instanceof Instant) {
          return DateTimeFormatter.ISO_INSTANT.format((Instant) value);
        }
        return value;
      case ENUM:
        // Symbol name as a String; pass through.
        return value;
      case UNION:
        // Encode using the first union branch's schema. Mirrors AvroData's
        // first-branch convention so all formats agree on which value type
        // a union default carries.
        return toJsonValue(type.getBranches().get(0).getSchema(), value);
      default:
        throw new ValidationException(
            "Default values are not supported for type: " + type.getType());
    }
  }

  /**
   * Decode a JSON-schema default value back into a typed Java value. Returns
   * {@code null} when the value's JSON type doesn't match the LT type — the
   * caller (the schema reader's default-capture path) treats a null return as
   * "drop this default" rather than failing the whole schema walk. This keeps
   * in-the-wild schemas with malformed defaults parseable.
   */
  public static Object toJavaData(final Schema type, final Object value) {
    return toJavaData(type, value, UnaryOperator.identity());
  }

  /**
   * {@link #toJavaData(Schema, Object)}, with each type first passed to {@code named}: a reference
   * to a named type, a composite's member included, is read as the type it names.
   */
  public static Object toJavaData(final Schema reference, final Object value,
      final UnaryOperator<Schema> named) {
    if (value == null) {
      return null;
    }
    final Schema type = named.apply(reference);
    switch (type.getType()) {
      case BOOLEAN:
        if (value instanceof Boolean) {
          return value;
        }
        break;
      case TINYINT:
        if (value instanceof Number) {
          return ((Number) value).byteValue();
        }
        break;
      case SMALLINT:
        if (value instanceof Number) {
          return ((Number) value).shortValue();
        }
        break;
      case INT:
        if (value instanceof Number) {
          return ((Number) value).intValue();
        }
        break;
      case BIGINT:
        if (value instanceof Number) {
          return ((Number) value).longValue();
        }
        break;
      case FLOAT:
        if (value instanceof Number) {
          return ((Number) value).floatValue();
        }
        break;
      case DOUBLE:
        if (value instanceof Number) {
          return ((Number) value).doubleValue();
        }
        break;
      case CHAR:
      case VARCHAR:
        if (value instanceof String) {
          return value;
        }
        break;
      case BINARY:
      case VARBINARY:
        if (value instanceof String) {
          return Base64.getDecoder().decode((String) value);
        }
        break;
      case DECIMAL:
        if (value instanceof String) {
          return new BigDecimal((String) value);
        } else if (value instanceof Number) {
          return new BigDecimal(value.toString());
        }
        break;
      case DATE:
        if (value instanceof String) {
          return LocalDate.parse((String) value);
        } else if (value instanceof Number) {
          // A Connect Date: days since the epoch.
          return LocalDate.ofEpochDay(((Number) value).longValue());
        }
        break;
      case TIME:
        if (value instanceof String) {
          return LocalTime.parse((String) value);
        } else if (value instanceof Number) {
          // A Connect Time: milliseconds of the day.
          return LocalTime.ofNanoOfDay(((Number) value).longValue() * 1_000_000L);
        }
        break;
      case TIMESTAMP:
        if (value instanceof String) {
          return LocalDateTime.parse((String) value);
        }
        break;
      case TIMESTAMP_LTZ:
        if (value instanceof String) {
          return Instant.from(DateTimeFormatter.ISO_INSTANT.parse((String) value));
        } else if (value instanceof Number) {
          return Instant.ofEpochMilli(((Number) value).longValue());
        }
        break;
      case ENUM:
        // Symbol name as a String.
        if (value instanceof String) {
          return value;
        }
        break;
      case UNION:
        // Decode using the first union branch's schema (encoder used the same).
        return toJavaData(type.getBranches().get(0).getSchema(), value, named);
      // Composites as Flink's JSON converter read them: a list, a map, a multiset's counts, a
      // struct's members by name.
      case ARRAY:
        if (value instanceof JSONArray || value instanceof List) {
          List<Object> list = new ArrayList<>();
          for (Object element : asList(value)) {
            list.add(member(type.getElementType(), element, named));
          }
          return list;
        }
        break;
      case MAP:
        return toMap(type.getKeyType(), type.getValueType(), value, named);
      case MULTISET:
        return toMap(type.getElementType(), Schema.create(Schema.Type.INT), value, named);
      case STRUCT:
        if (value instanceof JSONObject || value instanceof Map) {
          Map<?, ?> members = asMap(value);
          Map<String, Object> struct = new HashMap<>();
          for (Schema.Field field : type.getFields()) {
            Object member = member(field.getSchema(), members.get(field.getName()), named);
            if (member != null) {
              struct.put(field.getName(), member);
            }
          }
          return struct;
        }
        break;
      default:
        throw new ValidationException(
            "Default values are not supported for type: " + type.getType());
    }
    // Type mismatch: the value's Java type didn't match any branch above for
    // the requested LT type. Return null so the caller drops this default.
    return null;
  }

  /** Convenience: encode and produce a string suitable for ISO timestamp etc. */
  public static String toIsoInstant(Instant instant) {
    return DateTimeFormatter.ISO_INSTANT.format(instant);
  }

  // A map's default: an object keyed by string, or the entries a map with other keys is written as.
  private static Map<Object, Object> toMap(Schema keyType, Schema valueType, Object value,
      UnaryOperator<Schema> named) {
    Map<Object, Object> map = new HashMap<>();
    if (value instanceof JSONObject || value instanceof Map) {
      for (Map.Entry<?, ?> entry : asMap(value).entrySet()) {
        map.put(member(keyType, entry.getKey(), named),
            member(valueType, entry.getValue(), named));
      }
      return map;
    }
    if (value instanceof JSONArray || value instanceof List) {
      for (Object entry : asList(value)) {
        Map<?, ?> pair = entry instanceof JSONObject || entry instanceof Map ? asMap(entry) : null;
        if (pair == null || !pair.containsKey("key") || !pair.containsKey("value")) {
          throw new ValidationException("A map default's entries must have a key and a value");
        }
        map.put(member(keyType, pair.get("key"), named),
            member(valueType, pair.get("value"), named));
      }
      return map;
    }
    return null;
  }

  // A composite's member: null stays null, and one that is not a value of its type drops the whole.
  private static Object member(Schema type, Object value, UnaryOperator<Schema> named) {
    if (value == null || value == JSONObject.NULL) {
      return null;
    }
    Object member = toJavaData(type, value, named);
    if (member == null) {
      throw new ValidationException("A default's member is no " + type.getType() + " value");
    }
    return member;
  }

  private static List<?> asList(Object value) {
    return value instanceof JSONArray ? ((JSONArray) value).toList() : (List<?>) value;
  }

  private static Map<?, ?> asMap(Object value) {
    return value instanceof JSONObject ? ((JSONObject) value).toMap() : (Map<?, ?>) value;
  }
}
