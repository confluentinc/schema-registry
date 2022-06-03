/*
 * Copyright 2020 Confluent Inc.
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

    ArrayList<ObjectNode> arr = new ArrayList<>();
    for (Object message : messages) {
      Optional<ObjectNode> primitiveSchema = DeriveJsonSchemaPrimitive.getPrimitiveSchema(message);
      if (primitiveSchema.isPresent()) {
        arr.add(primitiveSchema.get());
      } else if (message instanceof ArrayNode) {
        arr.add(getSchemaForArray(DeriveSchema.getListFromArray(message), name));
      } else {
        ObjectNode objectNode = mapper.valueToTree(message);
        arr.add(DeriveJsonSchemaRecord.getSchemaForRecord(objectNode, name));
      }
    }

    return arr;
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
    ArrayList<ObjectNode> othersList = new ArrayList<>();

    for (ObjectNode schemaElement : uniqueSchemas) {
      if (schemaElement.get("type").asText().equals("object")) {
        recordList.add(schemaElement);
      } else if (schemaElement.get("type").asText().equals("array")) {
        arrayList.add(schemaElement);
      } else {
        othersList.add(schemaElement);
      }
    }

    if (recordList.size() > 1) {
      ObjectNode x = MergeJsonUtils.mergeRecords(recordList);
      othersList.add(x);
    } else if (recordList.size() == 1) {
      othersList.add(recordList.get(0));
    }

    if (arrayList.size() > 1) {
      ObjectNode x = MergeJsonUtils.mergeArrays(arrayList);
      othersList.add(x);
    } else if (arrayList.size() == 1) {
      othersList.add(arrayList.get(0));
    }

    if (othersList.size() > 1) {
      schema.set("items", concatElementsUsingOneOf(othersList));
    } else if (othersList.size() > 0) {
      schema.set("items", othersList.get(0));
    } else {
      schema.set("items", mapper.createObjectNode());
    }

    return schema;

  }

  private static ObjectNode concatElementsUsingOneOf(ArrayList<ObjectNode> othersList) {

    List<ObjectNode> elements = new ArrayList<>(othersList);
    ObjectNode oneOf = mapper.createObjectNode();
    ArrayNode arr = oneOf.putArray("oneOf");
    for (ObjectNode objectNode : elements) {
      arr.add(objectNode);
    }
    return oneOf;

  }


}
