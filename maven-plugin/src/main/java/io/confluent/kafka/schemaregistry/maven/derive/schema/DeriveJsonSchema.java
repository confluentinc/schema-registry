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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class DeriveJsonSchema extends DeriveSchema {

  public DeriveJsonSchema() {
    // Map jackson node data type to type understood by json
    classToDataType.put(com.fasterxml.jackson.databind.node.DoubleNode.class.getName(), "number");
    classToDataType.put(com.fasterxml.jackson.databind.node.TextNode.class.getName(), "string");
    classToDataType.put(com.fasterxml.jackson.databind.node.BigIntegerNode.class.getName(),
        "number");
    classToDataType.put(com.fasterxml.jackson.databind.node.IntNode.class.getName(), "number");
    classToDataType.put(com.fasterxml.jackson.databind.node.LongNode.class.getName(), "number");
    classToDataType.put(com.fasterxml.jackson.databind.node.BooleanNode.class.getName(),
        "boolean");
    classToDataType.put(com.fasterxml.jackson.databind.node.NullNode.class.getName(), "null");
    classToDataType.put(com.fasterxml.jackson.databind.node.MissingNode.class.getName(), "null");
  }

  public static ArrayNode sortJsonArrayList(ArrayNode node) {
    List<JsonNode> dataNodes = DeriveSchemaUtils.getListFromArray(node);
    // Sort items of arrayNode using type as the comparator
    List<JsonNode> sortedDataNodes = dataNodes
        .stream()
        .distinct()
        .sorted(Comparator.comparing(o -> o.get("type").asText()))
        .collect(Collectors.toList());
    return mapper.createObjectNode().arrayNode().addAll(sortedDataNodes);
  }

  protected ObjectNode mergeMultipleDataTypes(ObjectNode mergedArray,
                                              ArrayList<ObjectNode> primitives,
                                              ArrayList<ObjectNode> records,
                                              ArrayList<ObjectNode> arrays) {

    ArrayNode jsonItems = mapper.createArrayNode();
    // Adding primitive types to items' list
    for (ObjectNode item : primitives) {
      jsonItems.add(item);
    }
    // Merge records if there is at least 1 record
    if (records.size() > 0) {
      ObjectNode mergedRecords = mergeRecords(records);
      jsonItems.add(mergedRecords);
    }
    // Merge arrays if there is at least 1 array
    if (arrays.size() > 0) {
      ObjectNode mergedArrays = mergeArrays(arrays, true);
      jsonItems.add(mergedArrays);
    }

    if (jsonItems.size() > 1) {
      // If there are more than 1 different items, use oneOf to represent them
      ObjectNode oneOfDataType = mapper.createObjectNode();
      ArrayNode sortedJsonItems = sortJsonArrayList(jsonItems);
      oneOfDataType.set("oneOf", sortedJsonItems);
      mergedArray.set("items", oneOfDataType);
    } else if (jsonItems.size() == 1) {
      // Exactly one type of item, hence oneOf is not used
      mergedArray.set("items", jsonItems.get(0));
    } else {
      // No items found, setting items as empty object
      mergedArray.set("items", mapper.createObjectNode());
    }

    return mergedArray;
  }

  public ObjectNode getSchemaForMultipleMessages(List<String> messages)
      throws JsonProcessingException {
    /*
     Get schema for multiple messages. Exactly one schema is returned
     Treated same as array of records, the items derived is returned
     */
    List<JsonNode> messageObjects = new ArrayList<>();
    for (String message : messages) {
      messageObjects.add(mapper.readTree(message));
    }
    JsonNode schema = getSchemaForArray(messageObjects, "").get("items");
    ObjectNode schemaInformation = mapper.createObjectNode();
    schemaInformation.set("schema", schema);
    return schemaInformation;
  }
}
