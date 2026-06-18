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

package io.confluent.kafka.schemaregistry.type.logical.protobuf;

import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

/** Encodes/decodes default values for Protobuf as strings (stored in Meta params). */
public final class ProtoDefaultValueConverter {

  private ProtoDefaultValueConverter() {}

  /** Encode a typed Java default value into a string for storage in Meta.params. */
  public static String toProtoValue(final Schema type, final Object value) {
    if (value == null) {
      return "";
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
        return value.toString();
      case BINARY:
      case VARBINARY:
        if (value instanceof byte[]) {
          return Base64.getEncoder().encodeToString((byte[]) value);
        }
        return value.toString();
      case DECIMAL:
        if (value instanceof BigDecimal) {
          return value.toString();
        }
        return value.toString();
      case DATE:
        if (value instanceof LocalDate) {
          return value.toString();
        }
        return value.toString();
      case TIME:
        if (value instanceof LocalTime) {
          return value.toString();
        }
        return value.toString();
      case TIMESTAMP:
        if (value instanceof LocalDateTime) {
          return value.toString();
        }
        return value.toString();
      case TIMESTAMP_LTZ:
        if (value instanceof Instant) {
          return DateTimeFormatter.ISO_INSTANT.format((Instant) value);
        }
        return value.toString();
      case ENUM:
        // Symbol name as a String.
        return value.toString();
      case UNION:
        // Encode using the first union branch's schema. Mirrors AvroData's
        // first-branch convention so all formats agree on which value type
        // a union default carries.
        return toProtoValue(type.getBranches().get(0).getSchema(), value);
      default:
        throw new ValidationException(
            "Default values are not supported for type: " + type.getType());
    }
  }

  /** Decode a Meta.params default string back into a typed Java value. */
  public static Object toJavaData(final Schema type, final String value) {
    if (value == null) {
      return null;
    }
    switch (type.getType()) {
      case BOOLEAN:
        return Boolean.parseBoolean(value);
      case TINYINT:
        return Byte.parseByte(value);
      case SMALLINT:
        return Short.parseShort(value);
      case INT:
        return Integer.parseInt(value);
      case BIGINT:
        return Long.parseLong(value);
      case FLOAT:
        return Float.parseFloat(value);
      case DOUBLE:
        return Double.parseDouble(value);
      case CHAR:
      case VARCHAR:
        return value;
      case BINARY:
      case VARBINARY:
        return Base64.getDecoder().decode(value);
      case DECIMAL:
        return new BigDecimal(value);
      case DATE:
        return LocalDate.parse(value);
      case TIME:
        return LocalTime.parse(value);
      case TIMESTAMP:
        return LocalDateTime.parse(value);
      case TIMESTAMP_LTZ:
        return Instant.from(DateTimeFormatter.ISO_INSTANT.parse(value));
      case ENUM:
        // Symbol name as a String.
        return value;
      case UNION:
        // Decode using the first union branch's schema (encoder used the same).
        return toJavaData(type.getBranches().get(0).getSchema(), value);
      default:
        throw new ValidationException(
            "Default values are not supported for type: " + type.getType());
    }
  }
}
