/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.schema;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

public class ConnectUnion {

  public static final String LOGICAL_PARAMETER = "org.apache.kafka.connect.data.Union";

  /**
   * Returns a SchemaBuilder for a Union.
   *
   * @param annotation an arbitrary annotation to be associated with the union
   * @return a SchemaBuilder
   */
  public static SchemaBuilder builder(String annotation) {
    return SchemaBuilder.struct().parameter(LOGICAL_PARAMETER, annotation);
  }

  /**
   * Returns whether a schema represents a Union.
   *
   * @param schema the schema
   * @return whether the schema represents a Union
   */
  public static boolean isUnion(Schema schema) {
    return schema != null
        && schema.parameters() != null
        && schema.parameters().containsKey(LOGICAL_PARAMETER);
  }

  /**
   * Convert a value from its logical format (Union) to it's encoded format.
   *
   * @param schema the schema
   * @param value the logical value
   * @return the encoded value
   */
  public static Object fromLogical(Schema schema, Struct value) {
    if (!isUnion(schema)) {
      throw new DataException(
          "Requested conversion of Union object but the schema does not match.");
    }
    for (Field field : schema.fields()) {
      Object object = value.get(field);
      if (object != null) {
        return object;
      }
    }
    return null;
  }

  /**
   * Convert a value from its encoded format to its logical format (Union).
   * The value is associated with the field whose schema matches the given value.
   *
   * @param schema the schema
   * @param value the encoded value
   * @return the logical value
   */
  public static Struct toLogical(Schema schema, Object value) {
    if (!isUnion(schema)) {
      throw new DataException(
          "Requested conversion of Union object but the schema does not match.");
    }
    Struct struct = new Struct(schema);
    for (Field field : schema.fields()) {
      if (validate(field.schema(), value)) {
        struct.put(field, value);
        break;
      }
    }
    return struct;
  }

  private static boolean validate(Schema schema, Object value) {
    try {
      ConnectSchema.validateValue(schema, value);
    } catch (DataException e) {
      return false;
    }
    return true;
  }

  /**
   * Convert a value from its encoded format to its logical format (Union).
   * The value is associated with the field with the given field name.
   *
   * @param schema the schema
   * @param fieldName the field name
   * @param value the encoded value
   * @return the logical value
   */
  public static Struct toLogicalUsingName(Schema schema, String fieldName, Object value) {
    if (!isUnion(schema)) {
      throw new DataException(
          "Requested conversion of Union object but the schema does not match.");
    }
    Struct struct = new Struct(schema);
    for (Field field : schema.fields()) {
      if (field.name().equals(fieldName)) {
        struct.put(field, value);
        break;
      }
    }
    return struct;
  }
}
