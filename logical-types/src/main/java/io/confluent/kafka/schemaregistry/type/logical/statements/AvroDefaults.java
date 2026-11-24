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

package io.confluent.kafka.schemaregistry.type.logical.statements;

import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.EnumValue;
import io.confluent.kafka.schemaregistry.type.logical.avro.AvroDefaultValueConverter;
import java.nio.charset.StandardCharsets;

/**
 * An Avro attribute's default under a new type, as Avro reads a value written with the old type
 * under the new one: the same value, or one a promotion makes, a wider number or a string as its
 * bytes and back. Avro fills it in for records written before the attribute was, so it is what
 * those records then read.
 */
public final class AvroDefaults {

  // The Avro types an attribute is written as.
  private enum Kind { INT, LONG, FLOAT, DOUBLE, BOOLEAN, STRING, BYTES, ENUM, OTHER }

  private AvroDefaults() {
  }

  /** The default as the new type reads it; refused if Avro could not read it so. */
  public static Object converted(String name, Schema from, Schema to, Object value) {
    from = firstBranch(from);
    to = firstBranch(to);
    Kind source = kind(from);
    Kind target = kind(to);
    Object read = null;
    if (source == Kind.ENUM && target == Kind.ENUM) {
      // An enum's default is its symbol, which Avro reads by name; one of a type of its own,
      // only named here, the registry checks the new enum has.
      read = value instanceof String && (to.getType() != Schema.Type.ENUM
          || to.getEnumValues().stream().map(EnumValue::getSymbol).anyMatch(value::equals))
          ? value : null;
    } else if (source != Kind.ENUM && source != Kind.OTHER) {
      read = fits(to, promoted(source, target, written(from, value)));
    }
    RuntimeException cause = null;
    try {
      if (read != null) {
        return AvroDefaultValueConverter.toJavaData(to, read);
      }
    } catch (RuntimeException e) {
      // Out of the new type's range, as a time of day past midnight.
      cause = e;
    }
    throw new TypeRefusedException("Attribute " + name + "'s default " + value + " cannot be read "
        + "as " + to.getType() + ", as Avro reads a " + from.getType() + " written before "
        + "the change", cause);
  }

  // A whole number in the range of the new type, which Avro holds as an int whatever its range.
  private static Object fits(Schema to, Object datum) {
    long min = to.getType() == Schema.Type.TINYINT ? Byte.MIN_VALUE
        : to.getType() == Schema.Type.SMALLINT ? Short.MIN_VALUE : Long.MIN_VALUE;
    long max = to.getType() == Schema.Type.TINYINT ? Byte.MAX_VALUE
        : to.getType() == Schema.Type.SMALLINT ? Short.MAX_VALUE : Long.MAX_VALUE;
    return datum instanceof Integer && (((Integer) datum) < min || ((Integer) datum) > max)
        ? null : datum;
  }

  // A union's default is a value of its first branch.
  private static Schema firstBranch(Schema schema) {
    return schema.getType() == Schema.Type.UNION && !schema.getBranches().isEmpty()
        ? firstBranch(schema.getBranches().get(0).getSchema()) : schema;
  }

  private static Kind kind(Schema schema) {
    switch (schema.getType()) {
      case TINYINT:
      case SMALLINT:
      case INT:
      case DATE:
        return Kind.INT;
      case TIME:
        return schema.getPrecision() <= 3 ? Kind.INT : Kind.LONG;
      case BIGINT:
      case TIMESTAMP:
      case TIMESTAMP_LTZ:
        return Kind.LONG;
      case FLOAT:
        return Kind.FLOAT;
      case DOUBLE:
        return Kind.DOUBLE;
      case BOOLEAN:
        return Kind.BOOLEAN;
      case CHAR:
      case VARCHAR:
        return Kind.STRING;
      case BINARY:
      case VARBINARY:
      case DECIMAL:
        return Kind.BYTES;
      case ENUM:
      case NAMED_TYPE_REF:
        // A type of its own, as one made by CREATE TYPE, that can have a default: an enum.
        return Kind.ENUM;
      default:
        return Kind.OTHER;
    }
  }

  // The value as written: its Avro datum, bytes as a byte array.
  private static Object written(Schema type, Object value) {
    Object datum = AvroDefaultValueConverter.toAvroData(type, value);
    return kind(type) == Kind.BYTES && datum instanceof String
        ? ((String) datum).getBytes(StandardCharsets.ISO_8859_1) : datum;
  }

  // The datum read with the target's type, by Avro's promotions; null if Avro reads none.
  private static Object promoted(Kind source, Kind target, Object datum) {
    switch (target) {
      case INT:
        return source == Kind.INT ? ((Number) datum).intValue() : null;
      case LONG:
        return source == Kind.INT || source == Kind.LONG
            ? (Object) ((Number) datum).longValue() : null;
      case FLOAT:
        return source == Kind.INT || source == Kind.LONG || source == Kind.FLOAT
            ? (Object) ((Number) datum).floatValue() : null;
      case DOUBLE:
        return source == Kind.INT || source == Kind.LONG || source == Kind.FLOAT
            || source == Kind.DOUBLE ? (Object) ((Number) datum).doubleValue() : null;
      case BOOLEAN:
        return source == Kind.BOOLEAN ? datum : null;
      case STRING:
        return source == Kind.STRING ? datum : source == Kind.BYTES
            ? new String((byte[]) datum, StandardCharsets.UTF_8) : null;
      case BYTES:
        return source == Kind.BYTES ? datum : source == Kind.STRING
            ? ((String) datum).getBytes(StandardCharsets.UTF_8) : null;
      default:
        return null;
    }
  }
}
