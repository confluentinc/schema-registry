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
  public static final String PRIMITIVE_SCHEMA_TYPE = "{\"type\":\"%s\"}";

  protected List<JsonNode> getSchemaOfAllElements(List<JsonNode> messages, String name)
      throws JsonProcessingException {
    // Get schema of each message based on type
    List<JsonNode> schemaList = new ArrayList<>();
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

  private JsonNode getSchemaOfElement(JsonNode message, String name)
      throws JsonProcessingException {
    // Get schema of message based on type
    Optional<JsonNode> primitiveSchema = getPrimitiveSchema(message);
    if (primitiveSchema.isPresent()) {
      return primitiveSchema.get();
    } else if (message instanceof ArrayNode) {
      return getSchemaForArray(DeriveSchemaUtils.getListFromArray(message), name);
    } else {
      ObjectNode objectNode = mapper.valueToTree(message);
      return getSchemaForRecord(objectNode);
    }
  }

  public Optional<JsonNode> getPrimitiveSchema(JsonNode field) {
    // Generate Schema for Primitive data types
    String inferredType = field.getClass().getName();
    if (classToDataType.containsKey(inferredType)) {
      try {
        String schema = String.format(PRIMITIVE_SCHEMA_TYPE, classToDataType.get(inferredType));
        return Optional.of(mapper.readTree(schema));
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
    List<JsonNode> schemaList = getSchemaOfAllElements(messages, name);
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

  public ObjectNode mergeRecords(List<JsonNode> recordList) {
    // Merge all fields in all the records together into one record
    ObjectNode mergedRecord = mapper.createObjectNode();
    mergedRecord.put("type", "object");
    ObjectNode properties = mapper.createObjectNode();
    HashMap<String, List<JsonNode>> fieldToType = new HashMap<>();

    // Loop through every record and group them by field name
    // Then for each field, treat the list of schemas as array and try to merge
    for (JsonNode record : recordList) {
      JsonNode fields = record.get("properties");
      for (String fieldName : DeriveSchemaUtils.getSortedKeys(fields)) {
        List<JsonNode> listOfTypesForField = fieldToType.getOrDefault(fieldName, new ArrayList<>());
        listOfTypesForField.add(fields.get(fieldName));
        fieldToType.put(fieldName, listOfTypesForField);
      }
    }

    // Merging type for each field using fieldToType map
    for (Map.Entry<String, List<JsonNode>> entry : fieldToType.entrySet()) {
      List<JsonNode> fieldsType = fieldToType.get(entry.getKey());
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

  public ObjectNode mergeArrays(List<JsonNode> arrayList, boolean useItems,
                                boolean check2dArray) {
    // Merging different field types into one type
    ObjectNode mergedArray = mapper.createObjectNode();
    mergedArray.put("type", "array");
    List<JsonNode> primitives = new ArrayList<>();
    List<JsonNode> records = new ArrayList<>();
    List<JsonNode> arrays = new ArrayList<>();

    // Group items of array into record, array and primitive types
    for (JsonNode arrayElements : DeriveSchemaUtils.getUnique(arrayList)) {
      if (!useItems) {
        DeriveSchemaUtils.groupItems(arrayElements, primitives, records, arrays);
      } else {
        DeriveSchemaUtils.groupItems(arrayElements.get("items"), primitives, records, arrays);
      }
    }
    return mergeMultipleDataTypes(mergedArray, primitives, records, arrays, check2dArray);
  }

  protected abstract ObjectNode mergeMultipleDataTypes(ObjectNode mergedArray,
                                                       List<JsonNode> primitives,
                                                       List<JsonNode> records,
                                                       List<JsonNode> arrays,
                                                       boolean check2dArray);

  protected abstract ObjectNode getSchemaForMultipleMessages(List<String> messages)
      throws JsonProcessingException;
}
