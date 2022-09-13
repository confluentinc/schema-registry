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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema;

import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Optional;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema.mapper;
import static io.confluent.kafka.schemaregistry.maven.derive.schema.json.MergeJsonUtils.convertItemsToArrayItems;
import static io.confluent.kafka.schemaregistry.maven.derive.schema.json.MergeJsonUtils.mergeArrays;

public class DeriveJsonSchema {

  private static final HashMap<String, String> classToDataType = new HashMap<>();

  static {
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

  static void groupItems(ObjectNode element,
                         ArrayList<ObjectNode> items,
                         ArrayList<ObjectNode> records,
                         ArrayList<ObjectNode> arrays) {
    // Group items into records, arrays and primitive types
    if (element.get("type").asText().equals("object")) {
      records.add(element);
    } else if (element.get("type").asText().equals("array")) {
      arrays.add(element);
    } else if (!items.contains(element)) {
      items.add(element);
    }
  }

  public static Optional<ObjectNode> getPrimitiveSchema(Object field)
      throws JsonProcessingException {
    // Generate Schema for Primitive type
    String jsonInferredType = field.getClass().getName();
    if (classToDataType.containsKey(jsonInferredType)) {
      String schemaString = String.format("{\"type\" : \"%s\"}",
          classToDataType.get(jsonInferredType));
      return Optional.of(mapper.readValue(schemaString, ObjectNode.class));
    }
    return Optional.empty();
  }

  private static ArrayList<ObjectNode> getSchemaOfAllElements(List<JsonNode> messages, String name)
      throws JsonProcessingException {
    // Get schema of each message based on type
    ArrayList<ObjectNode> schemaList = new ArrayList<>();
    for (JsonNode message : messages) {
      schemaList.add(getSchemaOfElement(message, name));
    }
    return schemaList;
  }

  private static ObjectNode getSchemaOfElement(JsonNode message, String name)
      throws JsonProcessingException {
    // Get schema of message based on type
    Optional<ObjectNode> primitiveSchema = getPrimitiveSchema(message);
    if (primitiveSchema.isPresent()) {
      return primitiveSchema.get();
    } else if (message instanceof ArrayNode) {
      return getSchemaForArray(DeriveSchema.getListFromArray((ArrayNode) message), name);
    } else {
      ObjectNode objectNode = mapper.valueToTree(message);
      return getSchemaForRecord(objectNode);
    }
  }

  public static ObjectNode getSchemaForArray(List<JsonNode> messages, String name)
      throws JsonProcessingException {
    // Generate Schema for Array type
    ObjectNode schema = mapper.createObjectNode();
    schema.put("type", "array");
    ArrayList<ObjectNode> schemaList = getSchemaOfAllElements(messages, name);
    ObjectNode items = mergeArrays(convertItemsToArrayItems(schemaList));
    schema.set("items", items.get("items"));
    return schema;
  }

  public static ObjectNode getSchemaForRecord(ObjectNode message)
      throws JsonProcessingException {
    // Generate Schema for Record type
    ObjectNode schema = mapper.createObjectNode();
    schema.put("type", "object");
    schema.set("properties", mapper.createObjectNode());

    // Loop over each field, get type of each field and insert into schema
    for (String fieldName : DeriveSchema.getSortedKeys(message)) {
      JsonNode field = message.get(fieldName);
      ObjectNode fields = (ObjectNode) schema.get("properties");
      fields.set(fieldName, getSchemaOfElement(field, fieldName));
    }
    return schema;
  }

  public static ObjectNode getSchemaForMultipleMessages(List<String> messages)
      throws JsonProcessingException {
    /*
     Get schema for multiple messages. Exactly one schema is returned
     Treated same as array of records, the items derived is returned
     */
    List<JsonNode> messageObjects = new ArrayList<>();
    for (String message : messages) {
      messageObjects.add(mapper.readTree(message));
    }
    ObjectNode schema = (ObjectNode) getSchemaForArray(
        messageObjects, "").get("items");
    ObjectNode schemaInformation = mapper.createObjectNode();
    schemaInformation.set("schema", schema);
    return schemaInformation;
  }
}
