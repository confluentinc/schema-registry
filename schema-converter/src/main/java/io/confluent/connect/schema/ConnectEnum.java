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

import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;

public class ConnectEnum {

  public static final String LOGICAL_PARAMETER = "org.apache.kafka.connect.data.Enum";

  /**
   * Returns a SchemaBuilder for an Enum.
   *
   * @param annotation an arbitrary annotation to be associated with the enum
   * @param symbols the enum symbols
   * @return a SchemaBuilder
   */
  public static SchemaBuilder builder(String annotation, List<String> symbols) {
    SchemaBuilder builder = SchemaBuilder.string().parameter(LOGICAL_PARAMETER, annotation);
    for (int i = 0; i < symbols.size(); i++) {
      builder.parameter(LOGICAL_PARAMETER + "." + symbols.get(i), String.valueOf(i));
    }
    return builder;
  }

  /**
   * Returns a SchemaBuilder for an Enum.
   *
   * @param annotation an arbitrary annotation to be associated with the enum
   * @param symbols a map of enum symbol to its ordinal
   * @return a SchemaBuilder
   */
  public static SchemaBuilder builder(String annotation, Map<String, Integer> symbols) {
    SchemaBuilder builder = SchemaBuilder.string().parameter(LOGICAL_PARAMETER, annotation);
    for (Map.Entry<String, Integer> symbol : symbols.entrySet()) {
      builder.parameter(LOGICAL_PARAMETER + "." + symbol.getKey(),
          String.valueOf(symbol.getValue()));
    }
    return builder;
  }

  /**
   * Returns whether a schema represents an Enum.
   *
   * @param schema the schema
   * @return whether the schema represents an Enum
   */
  public static boolean isEnum(Schema schema) {
    return schema != null
        && schema.parameters() != null
        && schema.parameters().containsKey(LOGICAL_PARAMETER);
  }

  /**
   * Returns whether a schema has an Enum symbol.
   *
   * @param schema the schema
   * @param symbol the enum symbol
   * @return whether the schema represents an Enum
   */
  public static boolean hasEnumSymbol(Schema schema, String symbol) {
    return schema != null
        && schema.parameters() != null
        && schema.parameters().containsKey(LOGICAL_PARAMETER)
        && schema.parameters().containsKey(LOGICAL_PARAMETER + "." + symbol);
  }

  /**
   * Convert a value from its logical format (Enum) to its encoded format.
   *
   * @param schema the schema
   * @param value the logical value
   * @return the encoded value
   */
  public static <T extends java.lang.Enum<T>> String fromLogical(Schema schema, T value) {
    if (!hasEnumSymbol(schema, value.name())) {
      throw new DataException(
          "Requested conversion of Enum object but the schema does not match.");
    }
    return value.name();
  }

  /**
   * Convert a value from its encoded format to its logical format (Enum).
   *
   * @param schema the schema
   * @param cls the class of the logical value
   * @param symbol the enum symbol
   * @return the logical value
   */
  public static <T extends java.lang.Enum<T>> T toLogical(Schema schema, Class<T> cls,
      String symbol) {
    if (!hasEnumSymbol(schema, symbol)) {
      throw new DataException(
          "Requested conversion of Enum object but the schema does not match.");
    }
    return java.lang.Enum.valueOf(cls, symbol);
  }

  /**
   * Convert a value from its encoded format to its ordinal.
   *
   * @param schema the schema
   * @param symbol the enum symbol
   * @return the ordinal
   */
  public static int toOrdinal(Schema schema, String symbol) {
    if (!hasEnumSymbol(schema, symbol)) {
      throw new DataException(
          "Requested conversion of Enum object but the schema does not match.");
    }
    return Integer.parseInt(schema.parameters().get(LOGICAL_PARAMETER + "." + symbol));
  }
}
