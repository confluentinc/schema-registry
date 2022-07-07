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

package io.confluent.kafka.schemaregistry.maven.derive.schema.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema.mapper;

public class DeriveJsonSchemaArray {

  public static ArrayList<ObjectNode> getSchemaOfAllElements(List<Object> messages, String name)
      throws JsonProcessingException {

    ArrayList<ObjectNode> schemaList = new ArrayList<>();
    for (Object message : messages) {
      Optional<ObjectNode> primitiveSchema = DeriveJsonSchemaPrimitive.getPrimitiveSchema(message);
      if (primitiveSchema.isPresent()) {
        schemaList.add(primitiveSchema.get());
      } else if (message instanceof ArrayNode) {
        schemaList.add(getSchemaForArray(DeriveSchema.getListFromArray(message), name));
      } else {
        ObjectNode objectNode = mapper.valueToTree(message);
        schemaList.add(DeriveJsonSchemaRecord.getSchemaForRecord(objectNode, name));
      }
    }

    return schemaList;
  }

  /**
   * Generates schema for array type.
   * <p>
   * If the array contains multiple records,
   * they are merged together into one record with optional fields
   * </p>
   * <p>
   * If the array contains multiple arrays,
   * they are merged together into one array with multiple data types
   * </p>
   *
   * @param messages List of messages, each message is a ObjectNode
   * @param name     name assigned to array
   * @return schema
   * @throws JsonProcessingException thrown if message not in JSON format
   */
  public static ObjectNode getSchemaForArray(List<Object> messages, String name)
      throws JsonProcessingException {

    ObjectNode schema = mapper.createObjectNode();
    schema.put("type", "array");

    ArrayList<ObjectNode> schemaList = getSchemaOfAllElements(messages, name);
    ArrayList<ObjectNode> uniqueSchemas = DeriveSchema.getUnique(schemaList);
    ArrayList<ObjectNode> recordList = new ArrayList<>();
    ArrayList<ObjectNode> arrayList = new ArrayList<>();
    ArrayList<ObjectNode> primitiveList = new ArrayList<>();

    for (ObjectNode schemaElement : uniqueSchemas) {
      if (schemaElement.get("type").asText().equals("object")) {
        recordList.add(schemaElement);
      } else if (schemaElement.get("type").asText().equals("array")) {
        arrayList.add(schemaElement);
      } else {
        primitiveList.add(schemaElement);
      }
    }

    if (recordList.size() > 1) {
      ObjectNode mergedRecords = MergeJsonUtils.mergeRecords(recordList);
      primitiveList.add(mergedRecords);
    } else if (recordList.size() == 1) {
      primitiveList.add(recordList.get(0));
    }

    if (arrayList.size() > 1) {
      ObjectNode mergedArrays = MergeJsonUtils.mergeArrays(arrayList);
      primitiveList.add(mergedArrays);
    } else if (arrayList.size() == 1) {
      primitiveList.add(arrayList.get(0));
    }

    if (primitiveList.size() > 1) {
      schema.set("items", concatElementsUsingOneOf(primitiveList));
    } else if (primitiveList.size() > 0) {
      schema.set("items", primitiveList.get(0));
    } else {
      schema.set("items", mapper.createObjectNode());
    }

    return schema;
  }

  private static ObjectNode concatElementsUsingOneOf(ArrayList<ObjectNode> othersList) {

    List<ObjectNode> elements = new ArrayList<>(othersList);
    ObjectNode oneOfDataType = mapper.createObjectNode();
    ArrayNode oneOfElements = oneOfDataType.putArray("oneOf");
    for (ObjectNode objectNode : elements) {
      oneOfElements.add(objectNode);
    }
    return oneOfDataType;
  }

}
