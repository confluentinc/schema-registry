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

  static boolean isKeyValueSchema(Schema schema) {
    return schema.getType() == RECORD && schema.getFields().size() == 2;
  }

  /**
   * Build an Avro array-of-key-value-record schema with the {@code map}
   * logical type attached. Nullable key/value fields are given a
   * {@code default: null}.
   */
  public static Schema createMap(String keyValueName, Schema keySchema, Schema valueSchema) {
    Schema.Field keyField = new Schema.Field(
        "key", keySchema, null,
        isOptionSchema(keySchema) ? JsonProperties.NULL_VALUE : null);
    Schema.Field valueField = new Schema.Field(
        "value", valueSchema, null,
        isOptionSchema(valueSchema) ? JsonProperties.NULL_VALUE : null);
    return LogicalMap.get()
        .addToSchema(
            Schema.createArray(
                Schema.createRecord(
                    keyValueName, null, null, false, List.of(keyField, valueField))));
  }

  static boolean isOptionSchema(Schema schema) {
    if (schema.getType() == UNION && schema.getTypes().size() == 2) {
      if (schema.getTypes().get(0).getType() == Schema.Type.NULL) {
        return true;
      } else if (schema.getTypes().get(1).getType() == Schema.Type.NULL) {
        return true;
      }
    }
    return false;
  }
}
