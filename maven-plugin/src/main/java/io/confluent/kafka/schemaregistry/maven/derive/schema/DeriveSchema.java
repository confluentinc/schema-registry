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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Map;

public abstract class DeriveSchema {

  protected final HashMap<String, String> classToDataType = new HashMap<>();
  protected static final ObjectMapper mapper = JacksonMapper.INSTANCE;

  ArrayList<ObjectNode> getSchemaOfAllElements(List<JsonNode> messages, String name)
      throws JsonProcessingException {
    // Get schema of each message based on type
    ArrayList<ObjectNode> schemaList = new ArrayList<>();
    for (int i = 0; i < messages.size(); i++) {
      try {
        schemaList.add(getSchemaOfElement(messages.get(i), name));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format("unable to find schema for message %d: %s", i, messages.get(i)), e);
      }
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
      ObjectNode objectNode = mapper.valueToTree(message);
      return getSchemaForRecord(objectNode);
    }
  }

  public Optional<ObjectNode> getPrimitiveSchema(JsonNode field) {
    // Generate Schema for Primitive data types
    String inferredType = field.getClass().getName();
    if (classToDataType.containsKey(inferredType)) {
      try {
        String schema = String.format("{\"type\":\"%s\"}", classToDataType.get(inferredType));
        return Optional.of(mapper.readValue(schema, ObjectNode.class));
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }
    return Optional.empty();
  }

  public ObjectNode getSchemaForArray(List<JsonNode> messages, String name)
      throws JsonProcessingException {
    // Generate schema for array in json format, this schema is used as template by other formats
    ObjectNode schema = mapper.createObjectNode();
    schema.put("type", "array");
    List<ObjectNode> schemaList = getSchemaOfAllElements(messages, name);
    try {
      ObjectNode items = mergeArrays(schemaList, false, true);
      schema.set("items", items.get("items"));
      return schema;
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format("unable to find schema for array %s", name), e);
    }
  }

  public ObjectNode getSchemaForRecord(ObjectNode message)
      throws JsonProcessingException {
    // Generate schema for record in json format, this schema is used as template by other formats
    ObjectNode schema = mapper.createObjectNode();
    schema.put("type", "object");
    schema.set("properties", mapper.createObjectNode());

    // Loop over each field, get type of each field and insert into schema
    for (String fieldName : DeriveSchemaUtils.getSortedKeys(message)) {
      JsonNode field = message.get(fieldName);
      ObjectNode fields = (ObjectNode) schema.get("properties");
      fields.set(fieldName, getSchemaOfElement(field, fieldName));
    }
    return schema;
  }

  public ObjectNode mergeRecords(List<ObjectNode> recordList) {
    // Merge all fields in all the records together into one record
    ObjectNode mergedRecord = mapper.createObjectNode();
    mergedRecord.put("type", "object");
    ObjectNode properties = mapper.createObjectNode();
    HashMap<String, ArrayList<ObjectNode>> fieldToType = new HashMap<>();

    // Loop through every record and group them by field name
    // Then for each field, treat the list of schemas as array and try to merge
    for (ObjectNode record : recordList) {
      ObjectNode fields = (ObjectNode) record.get("properties");
      for (String fieldName : DeriveSchemaUtils.getSortedKeys(fields)) {
        ArrayList<ObjectNode> listOfTypesForField =
            fieldToType.getOrDefault(fieldName, new ArrayList<>());
        listOfTypesForField.add((ObjectNode) fields.get(fieldName));
        fieldToType.put(fieldName, listOfTypesForField);
      }
    }

    // Merging type for each field using fieldToType map
    for (Map.Entry<String, ArrayList<ObjectNode>> entry : fieldToType.entrySet()) {
      ArrayList<ObjectNode> fieldsType = fieldToType.get(entry.getKey());
      try {
        ObjectNode items = mergeArrays(fieldsType, false, false);
        properties.set(entry.getKey(), items.get("items"));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format("unable to merge field %s with types: %s",
                entry.getKey(), fieldsType), e);
      }
    }

    mergedRecord.set("properties", DeriveSchemaUtils.sortObjectNode(properties));
    return mergedRecord;
  }

  public ObjectNode mergeArrays(List<ObjectNode> arrayList, boolean useItems,
                                boolean check2dArray) {
    // Merging different field types into one type
    ObjectNode mergedArray = mapper.createObjectNode();
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
    return mergeMultipleDataTypes(mergedArray, primitives, records, arrays, check2dArray);
  }

  protected abstract ObjectNode mergeMultipleDataTypes(ObjectNode mergedArray,
                                                       ArrayList<ObjectNode> primitives,
                                                       ArrayList<ObjectNode> records,
                                                       ArrayList<ObjectNode> arrays,
                                                       boolean check2dArray);

  protected abstract ObjectNode getSchemaForMultipleMessages(List<String> messages)
      throws JsonProcessingException;
}
