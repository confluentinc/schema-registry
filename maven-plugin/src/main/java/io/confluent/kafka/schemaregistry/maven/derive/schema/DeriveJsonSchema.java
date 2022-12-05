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
import io.confluent.kafka.schemaregistry.json.JsonSchema;

import java.util.List;
import java.util.Map;

public class DeriveJsonSchema extends DeriveSchema {

  public static final String NUMBER = "number";
  public static final String STRING = "string";
  public static final String BOOLEAN = "boolean";
  public static final String NULL = "null";

  public DeriveJsonSchema() {
    // Map jackson node data type to type understood by json
    classToDataType.put(com.fasterxml.jackson.databind.node.DoubleNode.class.getName(), NUMBER);
    classToDataType.put(com.fasterxml.jackson.databind.node.TextNode.class.getName(), STRING);
    classToDataType.put(com.fasterxml.jackson.databind.node.BigIntegerNode.class.getName(),
        NUMBER);
    classToDataType.put(com.fasterxml.jackson.databind.node.IntNode.class.getName(), NUMBER);
    classToDataType.put(com.fasterxml.jackson.databind.node.LongNode.class.getName(), NUMBER);
    classToDataType.put(com.fasterxml.jackson.databind.node.BooleanNode.class.getName(),
        BOOLEAN);
    classToDataType.put(com.fasterxml.jackson.databind.node.NullNode.class.getName(), NULL);
    classToDataType.put(com.fasterxml.jackson.databind.node.MissingNode.class.getName(), NULL);
  }

  /**
   * Merge different records into one record and different arrays into one array
   * Multiple data types are combined through oneOf
   */
  protected ObjectNode mergeMultipleDataTypes(ObjectNode mergedArray,
                                              List<JsonNode> primitives,
                                              List<JsonNode> records,
                                              List<JsonNode> arrays,
                                              boolean check2dArray) {
    ArrayNode items = mapper.createArrayNode().addAll(primitives);
    // Merge records if there is at least 1 record
    if (records.size() > 0) {
      items.add(mergeRecords(records));
    }
    // Merge arrays if there is at least 1 array
    if (arrays.size() > 0) {
      items.add(mergeArrays(arrays, true, false));
    }

    if (items.size() > 1) {
      // If there are more than 1 different items, use oneOf to represent them
      ObjectNode oneOfDataType = mapper.createObjectNode().set("oneOf",
          DeriveSchemaUtils.sortJsonArrayList(items));
      mergedArray.set("items", oneOfDataType);
    } else if (items.size() == 1) {
      mergedArray.set("items", items.get(0));
    } else {
      // No items found, setting items as empty object
      mergedArray.set("items", mapper.createObjectNode());
    }

    return mergedArray;
  }

  /**
   * Merge the unique schemas into one record by combining fields and merging data types
   */
  @Override
  public ArrayNode mergeMultipleMessages(List<JsonNode> uniqueSchemas,
                                         Map<JsonNode, ArrayNode> schemaToIndex) {
    JsonNode schema = mergeRecords(uniqueSchemas);
    ArrayNode messagesMatched = mapper.createArrayNode();
    int totalSize = 0;
    // Find total number of schemas and set that as messages matched, so [0 .. n-1]
    for (Map.Entry<JsonNode, ArrayNode> entry : schemaToIndex.entrySet()) {
      totalSize += entry.getValue().size();
    }
    for (int i = 0; i < totalSize; i++) {
      messagesMatched.add(i);
    }
    ArrayNode schemaInfoList = mapper.createArrayNode();
    updateSchemaInformation(schema, messagesMatched, schemaInfoList);
    return schemaInfoList;
  }

  /**
   * Generate json schema and check for any errors
   */
  protected JsonNode convertToFormat(JsonNode schema, String name) {
    JsonSchema jsonSchema = new JsonSchema(schema);
    jsonSchema.validate();
    // Input schema is in json format, hence no conversion is needed. Returning schema as is
    return schema;
  }
}
