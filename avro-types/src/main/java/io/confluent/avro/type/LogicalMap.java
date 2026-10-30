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

package io.confluent.avro.type;

import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.avro.Schema.Type.UNION;

import java.util.List;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

public class LogicalMap extends LogicalType {
  public static final String NAME = "map";
  private static final LogicalMap INSTANCE = new LogicalMap();

  public static LogicalMap get() {
    return INSTANCE;
  }

  private LogicalMap() {
    super(NAME);
  }

  @Override
  public void validate(Schema schema) {
    super.validate(schema);
    if (schema.getType() != Schema.Type.ARRAY) {
      throw new IllegalArgumentException("Invalid type for map, must be an array: " + schema);
    }
    if (!isKeyValueSchema(schema.getElementType())) {
      throw new IllegalArgumentException("Invalid key-value record: " + schema.getElementType());
    }
  }

  /**
   * The element schema of a {@code map} logical-typed array must be a record
   * whose first field is named {@code key} and whose second field is named
   * {@code value}. The conversion ({@link LogicalMapConversion}) reads/writes
   * those fields positionally, so the names+positions are part of the on-the-
   * wire contract — relaxing the check here would let mis-shaped schemas pass
   * validation and silently swap key↔value (or write key data into a
   * differently-typed field) at conversion time.
   */
  static boolean isKeyValueSchema(Schema schema) {
    if (schema.getType() != RECORD || schema.getFields().size() != 2) {
      return false;
    }
    return "key".equals(schema.getFields().get(0).name())
        && "value".equals(schema.getFields().get(1).name());
  }

  /**
   * Build an Avro array-of-key-value-record schema with the {@code map}
   * logical type attached. Fields whose schema is a {@code [null, X]} union
   * (null-first) are given a {@code default: null}; {@code [X, null]} unions
   * are left undefaulted because the Avro spec requires a union default to
   * match the *first* branch.
   */
  public static Schema createMap(String keyValueName, Schema keySchema, Schema valueSchema) {
    Schema.Field keyField = new Schema.Field(
        "key", keySchema, null,
        isNullFirstUnion(keySchema) ? JsonProperties.NULL_VALUE : null);
    Schema.Field valueField = new Schema.Field(
        "value", valueSchema, null,
        isNullFirstUnion(valueSchema) ? JsonProperties.NULL_VALUE : null);
    return LogicalMap.get()
        .addToSchema(
            Schema.createArray(
                Schema.createRecord(
                    keyValueName, null, null, false, List.of(keyField, valueField))));
  }

  static boolean isNullFirstUnion(Schema schema) {
    return schema.getType() == UNION
        && schema.getTypes().size() == 2
        && schema.getTypes().get(0).getType() == Schema.Type.NULL;
  }
}
