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
import java.util.Base64;

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

  /** Decode a JSON-schema default value back into a typed Java value. */
  public static Object toJavaData(final Schema type, final Object value) {
    if (value == null) {
      return null;
    }
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
        }
        break;
      case TIME:
        if (value instanceof String) {
          return LocalTime.parse((String) value);
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
        return toJavaData(type.getBranches().get(0).getSchema(), value);
      default:
        throw new ValidationException(
            "Default values are not supported for type: " + type.getType());
    }
    return value;
  }

  /** Convenience: encode and produce a string suitable for ISO timestamp etc. */
  public static String toIsoInstant(Instant instant) {
    return DateTimeFormatter.ISO_INSTANT.format(instant);
  }
}
