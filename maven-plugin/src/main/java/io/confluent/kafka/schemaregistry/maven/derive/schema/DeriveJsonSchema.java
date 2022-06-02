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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.maven.derive.schema.utils.MergeJsonUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.List;


/**
 * Class with functionality to derive schema from messages for JSON.
 */
public class DeriveJsonSchema extends DeriveSchema {


  public DeriveJsonSchema() {
    fillMap();
  }

  final Map<String, String> classToDataType = new HashMap<>();

  private void fillMap() {

    classToDataType.put("class com.fasterxml.jackson.databind.node.IntNode", "number");
    classToDataType.put("class com.fasterxml.jackson.databind.node.LongNode", "number");
    classToDataType.put("class com.fasterxml.jackson.databind.node.BooleanNode", "boolean");
    classToDataType.put("class com.fasterxml.jackson.databind.node.NullNode", "null");
    classToDataType.put("class com.fasterxml.jackson.databind.node.DoubleNode", "number");
    classToDataType.put("class com.fasterxml.jackson.databind.node.TextNode", "string");
    classToDataType.put("class com.fasterxml.jackson.databind.node.BigIntegerNode", "number");

  }

  /**
   * Primitive schema of message is generated, if possible.
   *
   * @param field - message whose schema has to be found
   * @return ObjectNode if type primitive else empty option
   */
  Optional<ObjectNode> getPrimitiveSchema(Object field) throws JsonProcessingException {

    String jsonInferredType;
    if (field == null) {
      jsonInferredType = "class com.fasterxml.jackson.databind.node.NullNode";
    } else {
      jsonInferredType = field.getClass().toString();
    }

    if (classToDataType.containsKey(jsonInferredType)) {
      String schemaString = String.format("{\"type\" : \"%s\"}",
          classToDataType.get(jsonInferredType));
      return Optional.of(mapper.readValue(schemaString, ObjectNode.class));
    }

    return Optional.empty();
  }

  /**
   * Get schema for each message present in list.
   *
   * @param messages list of messages, each message is a ObjectNode
   * @param name     name used for record and arrays
   * @return List of Schemas, one schema for each message
   * @throws JsonProcessingException thrown if message not in JSON format
   */

  public ArrayList<ObjectNode> getSchemaOfAllElements(List<Object> messages, String name)
      throws JsonProcessingException {

    ArrayList<ObjectNode> arr = new ArrayList<>();
    for (Object message : messages) {
      Optional<ObjectNode> primitiveSchema = getPrimitiveSchema(message);
      if (primitiveSchema.isPresent()) {
        arr.add(primitiveSchema.get());
      } else if (message instanceof ArrayNode) {
        arr.add(getSchemaForArray(getListFromArray(message), name));
      } else {
        ObjectNode objectNode = mapper.valueToTree(message);
        arr.add(getSchemaForRecord(objectNode, name));
      }
    }

    return arr;
  }


  private ObjectNode concatElementsUsingOneOf(ArrayList<ObjectNode> othersList) {

    List<ObjectNode> elements = new ArrayList<>(othersList);
    ObjectNode oneOf = mapper.createObjectNode();
    ArrayNode arr = oneOf.putArray("oneOf");
    for (ObjectNode objectNode : elements) {
      arr.add(objectNode);
    }
    return oneOf;

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

  public ObjectNode getSchemaForArray(List<Object> messages, String name)
      throws JsonProcessingException {

    ObjectNode schema = mapper.createObjectNode();
    schema.put("type", "array");

    ArrayList<ObjectNode> schemaList = getSchemaOfAllElements(messages, name);
    ArrayList<ObjectNode> uniqueSchemas = MergeJsonUtils.getUnique(schemaList);
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

  /**
   * Generates schema for record.
   *
   * @param message input message for which schema is found
   * @param name    name assigned to record
   * @return schema
   * @throws JsonProcessingException thrown if message not in JSON format
   */
  public ObjectNode getSchemaForRecord(ObjectNode message, String name)
      throws JsonProcessingException {

    ObjectNode schema = mapper.createObjectNode();
    schema.put("type", "object");
    schema.set("properties", mapper.createObjectNode());

    for (String key : getSortedKeys(message)) {

      Object field = message.get(key);

      Optional<ObjectNode> primitiveSchema = getPrimitiveSchema(field);
      ObjectNode info;

      if (primitiveSchema.isPresent()) {
        info = primitiveSchema.get();
      } else {
        if (field instanceof ArrayNode) {
          info = getSchemaForArray(getListFromArray(field), key);
        } else {
          info = getSchemaForRecord(mapper.valueToTree(field), key);
        }
      }

      ObjectNode fields = (ObjectNode) schema.get("properties");
      fields.set(key, info);

    }

    return schema;
  }

  /**
   * Get schema for multiple messages.
   * Treated same as array of records and exactly one schema is returned
   *
   * @param messages list of messages, each message is a string
   * @return map with schema and the number of messages it matches
   * @throws JsonProcessingException thrown if message not in JSON format
   */
  public ObjectNode getSchemaForMultipleMessages(List<String> messages)
      throws JsonProcessingException {

    List<Object> messageObjects = new ArrayList<>();
    for (String s : messages) {
      messageObjects.add(mapper.readTree(s));
    }

    ObjectNode schema = (ObjectNode) getSchemaForArray(messageObjects, "").get("items");
    ObjectNode ans = mapper.createObjectNode();
    ans.set("schema", schema);
    return ans;
  }


}
