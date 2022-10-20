/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.maven.derive.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

import java.util.List;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchemaUtils.getSortedKeys;

public class DeriveProtobufSchema extends DeriveSchema {

  public static final String DOUBLE = "double";
  public static final String STRING = "string";
  public static final String BOOL = "bool";
  public static final String INT_32 = "int32";
  public static final String INT_64 = "int64";
  public static final String ANY_FIELD = "google.protobuf.Any";
  public static final String FIELD_ENTRY = " %s %s = %d;%n";
  public static final String PROTOBUF_SYNTAX = "syntax = \"proto3\";\n";

  public DeriveProtobufSchema() {
    // Map jackson node data type to type understood by protobuf
    classToDataType.put(com.fasterxml.jackson.databind.node.DoubleNode.class.getName(), DOUBLE);
    classToDataType.put(com.fasterxml.jackson.databind.node.TextNode.class.getName(), STRING);
    classToDataType.put(com.fasterxml.jackson.databind.node.BigIntegerNode.class.getName(), DOUBLE);
    classToDataType.put(com.fasterxml.jackson.databind.node.IntNode.class.getName(), INT_32);
    classToDataType.put(com.fasterxml.jackson.databind.node.LongNode.class.getName(), INT_64);
    classToDataType.put(com.fasterxml.jackson.databind.node.BooleanNode.class.getName(), BOOL);
    classToDataType.put(com.fasterxml.jackson.databind.node.NullNode.class.getName(), ANY_FIELD);
    classToDataType.put(com.fasterxml.jackson.databind.node.MissingNode.class.getName(), ANY_FIELD);
  }

  protected ObjectNode mergeMultipleDataTypes(ObjectNode mergedArray,
                                              List<JsonNode> primitives,
                                              List<JsonNode> records,
                                              List<JsonNode> arrays,
                                              boolean check2dArray) {
    ArrayNode items = mapper.createArrayNode();
    // Check for nested array, if yes return error
    if (arrays.size() > 0 && check2dArray) {
      throw new IllegalArgumentException(String.format("found nested array: %s", arrays));
    }

    DeriveSchemaUtils.mergeNumberTypes(primitives);
    // Adding primitive types to items' list
    items.addAll(DeriveSchemaUtils.getUnique(primitives));
    // No merging of arrays, add directly to items' list
    items.addAll(arrays);

    // Merge records if there is at least 1 record
    if (records.size() > 0) {
      items.add(mergeRecords(records));
    }

    if (items.size() > 1) {
      throw new IllegalArgumentException(String.format("found multiple data types: %s", items));
    } else if (items.size() == 1) {
      mergedArray.set("items", items.get(0));
    } else {
      mergedArray.set("items", getNullSchema());
    }
    return mergedArray;
  }

  @Override
  public TextNode convertToFormat(JsonNode schema, String name) {
    // Json schema is used as a template. Data types and array merging is defined specifically for
    // protobuf(mergeMultipleDataTypes and classToDataType), this converts json template to protobuf
    String schemaForRecord = convertToFormatRecord(schema, name);
    StringBuilder schemaBuilder = new StringBuilder(PROTOBUF_SYNTAX);
    if (schemaForRecord.contains(ANY_FIELD)) {
      schemaBuilder.append("import \"google/protobuf/any.proto\";\n");
    }
    schemaBuilder.append(schemaForRecord);
    ProtobufSchema protobufSchema = new ProtobufSchema(schemaBuilder.toString());
    protobufSchema.validate();
    return mapper.convertValue(protobufSchema.toString(), TextNode.class);
  }

  protected String convertToFormatRecord(JsonNode schema, String name) {
    // Convert json schema for record to protobuf schema
    int fieldNum = 1;
    StringBuilder protobufSchema = new StringBuilder();
    protobufSchema.append(String.format("message %s { %n", name));
    JsonNode properties = schema.get("properties");
    for (String fieldName : getSortedKeys(properties)) {
      JsonNode field = properties.get(fieldName);
      String fieldType = field.get("type").asText();
      switch (fieldType) {
        case "array":
          protobufSchema.append(convertToFormatArray(field, fieldName, fieldNum++));
          break;
        case "object":
          String recursiveRecord = convertToFormatRecord(field, fieldName + "Message");
          protobufSchema.append(recursiveRecord);
          protobufSchema.append(
              String.format(FIELD_ENTRY, fieldName + "Message", fieldName, fieldNum++));
          break;
        default:
          protobufSchema.append(String.format(FIELD_ENTRY, field.get("type").asText(),
              fieldName, fieldNum++));
          break;
      }
    }
    protobufSchema.append("}\n");
    return protobufSchema.toString();
  }

  protected String convertToFormatArray(JsonNode schema, String name, int fieldNum) {
    // Convert json schema for array to protobuf schema
    StringBuilder protobufSchema = new StringBuilder();
    JsonNode items = schema.get("items");
    String itemsType = items.get("type").asText();
    if (itemsType.equals("object")) {
      String recursiveRecord = convertToFormatRecord(items, name + "Message");
      protobufSchema.append(recursiveRecord);
      protobufSchema.append(
          String.format("repeated" + FIELD_ENTRY, name + "Message", name, fieldNum));
    } else {
      protobufSchema.append(String.format("repeated" + FIELD_ENTRY, itemsType, name, fieldNum));
    }
    return protobufSchema.toString();
  }
}
