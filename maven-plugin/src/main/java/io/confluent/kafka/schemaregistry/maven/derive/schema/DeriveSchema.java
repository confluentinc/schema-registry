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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Map;

public abstract class DeriveSchema {

  protected final HashMap<String, String> classToDataType = new HashMap<>();

  private ArrayList<ObjectNode> getSchemaOfAllElements(List<JsonNode> messages, String name)
      throws JsonProcessingException {
    // Get schema of each message based on type
    ArrayList<ObjectNode> schemaList = new ArrayList<>();
    for (JsonNode message : messages) {
      schemaList.add(getSchemaOfElement(message, name));
    }
    return schemaList;
  }

  private ObjectNode getSchemaOfElement(JsonNode message, String name)
      throws JsonProcessingException {
    // Get schema of message based on type
    Optional<ObjectNode> primitiveSchema = getPrimitiveSchema(message);
    if (primitiveSchema.isPresent()) {
      return primitiveSchema.get();
    } else if (message instanceof ArrayNode) {
      return getSchemaForArray(DeriveSchemaUtils.getListFromArray((ArrayNode) message), name);
    } else {
      ObjectNode objectNode = DeriveSchemaUtils.mapper.valueToTree(message);
      return getSchemaForRecord(objectNode);
    }
  }

  public Optional<ObjectNode> getPrimitiveSchema(Object field)
      throws JsonProcessingException {
    // Generate Schema for Primitive data types
    String jsonInferredType = field.getClass().getName();
    if (classToDataType.containsKey(jsonInferredType)) {
      String schemaString = String.format("{\"type\" : \"%s\"}",
          classToDataType.get(jsonInferredType));
      return Optional.of(DeriveSchemaUtils.mapper.readValue(schemaString, ObjectNode.class));
    }
    return Optional.empty();
  }

  public ObjectNode getSchemaForArray(List<JsonNode> messages, String name)
      throws JsonProcessingException {
    // Generate Schema for Array type
    ObjectNode schema = DeriveSchemaUtils.mapper.createObjectNode();
    schema.put("type", "array");
    ArrayList<ObjectNode> schemaList = getSchemaOfAllElements(messages, name);
    ObjectNode items = mergeArrays(schemaList, false);
    schema.set("items", items.get("items"));
    return schema;
  }

  public ObjectNode getSchemaForRecord(ObjectNode message)
      throws JsonProcessingException {
    // Generate Schema for Record type
    ObjectNode schema = DeriveSchemaUtils.mapper.createObjectNode();
    schema.put("type", "object");
    schema.set("properties", DeriveSchemaUtils.mapper.createObjectNode());

    // Loop over each field, get type of each field and insert into schema
    for (String fieldName : DeriveSchemaUtils.getSortedKeys(message)) {
      JsonNode field = message.get(fieldName);
      ObjectNode fields = (ObjectNode) schema.get("properties");
      fields.set(fieldName, getSchemaOfElement(field, fieldName));
    }
    return schema;
  }

  public ObjectNode mergeRecords(ArrayList<ObjectNode> recordList) {
    // Merge all fields in all the records together into one record
    ObjectNode mergedRecord = DeriveSchemaUtils.mapper.createObjectNode();
    mergedRecord.put("type", "object");
    ObjectNode properties = DeriveSchemaUtils.mapper.createObjectNode();
    HashMap<String, ArrayList<ObjectNode>> fieldToItsType = new HashMap<>();

    /*
      Loop through every record group schemas by field name
      Then for each field treat the list of schemas as array and try to merge
    */
    for (ObjectNode record : recordList) {
      ObjectNode fields = (ObjectNode) record.get("properties");
      for (String fieldName : DeriveSchemaUtils.getSortedKeys(fields)) {
        ArrayList<ObjectNode> listOfTypesForField =
            fieldToItsType.getOrDefault(fieldName, new ArrayList<>());
        listOfTypesForField.add((ObjectNode) fields.get(fieldName));
        fieldToItsType.put(fieldName, listOfTypesForField);
      }
    }

    // Merging type for each field using fieldToItsType map
    for (Map.Entry<String, ArrayList<ObjectNode>> entry : fieldToItsType.entrySet()) {
      ArrayList<ObjectNode> fieldsType = fieldToItsType.get(entry.getKey());
      ObjectNode items = mergeArrays(fieldsType, false);
      properties.set(entry.getKey(), items.get("items"));
    }

    mergedRecord.set("properties", DeriveSchemaUtils.sortObjectNode(properties));
    return mergedRecord;
  }

  public ObjectNode mergeArrays(ArrayList<ObjectNode> arrayList, boolean useItems) {
    // Merging different field types into one type
    ObjectNode mergedArray = DeriveSchemaUtils.mapper.createObjectNode();
    mergedArray.put("type", "array");

    ArrayList<ObjectNode> primitives = new ArrayList<>();
    ArrayList<ObjectNode> records = new ArrayList<>();
    ArrayList<ObjectNode> arrays = new ArrayList<>();

    // Group items of array into record, array and primitive types
    for (ObjectNode arrayElements : DeriveSchemaUtils.getUnique(arrayList)) {
      if (!useItems) {
        DeriveSchemaUtils.groupItems(arrayElements, primitives, records, arrays);
      } else {
        DeriveSchemaUtils.groupItems(
            (ObjectNode) arrayElements.get("items"), primitives, records, arrays);
      }
    }
    return mergeMultipleDataTypes(mergedArray, primitives, records, arrays);
  }

  protected abstract ObjectNode mergeMultipleDataTypes(ObjectNode mergedArray,
                                                       ArrayList<ObjectNode> primitives,
                                                       ArrayList<ObjectNode> records,
                                                       ArrayList<ObjectNode> arrays);

  protected abstract ObjectNode getSchemaForMultipleMessages(List<String> messages)
      throws JsonProcessingException;
}
