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
import com.fasterxml.jackson.databind.node.NullNode;
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
    List<JsonNode> schemaList = new ArrayList<>();
    for (int i = 0; i < messages.size(); i++) {
      try {
        schemaList.add(getSchemaOfElement(messages.get(i), name));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format("Unable to find schema for message %d: %s", i, messages.get(i)), e);
      }
    }
    return schemaList;
  }

  private JsonNode getSchemaOfElement(JsonNode message, String name)
      throws JsonProcessingException {
    Optional<JsonNode> primitiveSchema = getPrimitiveSchema(message);
    checkName(name);
    if (primitiveSchema.isPresent()) {
      return primitiveSchema.get();
    } else if (message instanceof ArrayNode) {
      return getSchemaForArray(DeriveSchemaUtils.getListFromArray(message), name);
    } else {
      return getSchemaForRecord(mapper.valueToTree(message));
    }
  }

  protected void checkName(String name) {
    // Do nothing, json has no name restrictions and avro errors are suggestive
    // Overridden by protobuf to throw naming errors
  }

  public Optional<JsonNode> getPrimitiveSchema(JsonNode field) {
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

  /**
   * Generate schema for array in json format, this schema is used as template by other formats
   */
  public ObjectNode getSchemaForArray(List<JsonNode> messages, String name)
      throws JsonProcessingException {
    ObjectNode schema = mapper.createObjectNode().put("type", "array");
    List<JsonNode> schemaList = getSchemaOfAllElements(messages, name);
    try {
      ObjectNode items = mergeArrays(schemaList, false, true);
      schema.set("items", items.get("items"));
      return schema;
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format("Unable to find schema for array %s", name), e);
    }
  }

  /**
   * Generate schema for record in json format, this schema is used as template by other formats
   */
  public ObjectNode getSchemaForRecord(ObjectNode message)
      throws JsonProcessingException {
    ObjectNode schema = mapper.createObjectNode().put("type", "object");
    schema.set("properties", mapper.createObjectNode());

    // Loop over each field, get type of each field and insert into schema
    for (String fieldName : DeriveSchemaUtils.getSortedKeys(message)) {
      JsonNode field = message.get(fieldName);
      ObjectNode fields = (ObjectNode) schema.get("properties");
      fields.set(fieldName, getSchemaOfElement(field, fieldName));
    }
    return schema;
  }

  /**
   * Merge fields in all the records together into one record
   */
  public ObjectNode mergeRecords(List<JsonNode> recordList) {
    ObjectNode properties = mapper.createObjectNode();
    Map<String, List<JsonNode>> fieldToType = new HashMap<>();

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

    IllegalArgumentException mergingError = null;
    String mergingErrorField = null;
    // Merging type for each field using fieldToType map
    for (Map.Entry<String, List<JsonNode>> entry : fieldToType.entrySet()) {
      String fieldName = entry.getKey();
      List<JsonNode> fieldTypes = fieldToType.get(fieldName);
      try {
        ObjectNode items = mergeArrays(fieldTypes, false, false).deepCopy();
        properties.set(fieldName, items.get("items"));
        // Editing fields in-place and replacing with merged item
        DeriveSchemaUtils.replaceEachField(items.get("items"), fieldTypes);
      } catch (IllegalArgumentException e) {
        // Capture error and field here, thrown later to allow loop completion
        mergingError = e;
        mergingErrorField = fieldName;
      }
    }

    if (mergingErrorField != null) {
      throw new IllegalArgumentException(String.format("Unable to merge field %s with types: %s",
          mergingErrorField, fieldToType.get(mergingErrorField)), mergingError);
    }

    // Return object with all fields combined into 1 object
    ObjectNode mergedRecord = mapper.createObjectNode().put("type", "object");
    mergedRecord.set("properties", DeriveSchemaUtils.sortObjectNode(properties));
    return mergedRecord;
  }

  /**
   * Merging different field types of array into one type
   */
  public ObjectNode mergeArrays(List<JsonNode> arrayList, boolean useItems, boolean check2dArray) {
    ObjectNode mergedArray = mapper.createObjectNode().put("type", "array");
    List<JsonNode> primitives = new ArrayList<>();
    List<JsonNode> records = new ArrayList<>();
    List<JsonNode> arrays = new ArrayList<>();

    // Group items in the array into record, array and primitive types
    for (JsonNode arrayElements : DeriveSchemaUtils.getUnique(arrayList)) {
      if (!useItems) {
        DeriveSchemaUtils.groupItems(arrayElements, primitives, records, arrays);
      } else {
        DeriveSchemaUtils.groupItems(arrayElements.get("items"), primitives, records, arrays);
      }
    }
    return mergeMultipleDataTypes(mergedArray, primitives, records, arrays, check2dArray);
  }

  protected JsonNode getNullSchema() {
    return getPrimitiveSchema(mapper.convertValue(null, NullNode.class)).get();
  }

  public ObjectNode getSchemaForMultipleMessages(List<JsonNode> messages)
      throws JsonProcessingException {
    List<JsonNode> schemas = getSchemaOfAllElements(messages, "Schema");
    // Use only unique schemas and collect all indices it matches
    List<JsonNode> uniqueSchemas = DeriveSchemaUtils.getUnique(schemas);
    Map<JsonNode, ArrayNode> schemaToIndex = new HashMap<>();
    uniqueSchemas.forEach(s -> schemaToIndex.put(s, mapper.createArrayNode()));
    for (int i = 0; i < schemas.size(); i++) {
      schemaToIndex.get(schemas.get(i)).add(i);
    }
    ArrayNode schemaInfoList = mergeMultipleMessages(uniqueSchemas, schemaToIndex);
    return mapper.createObjectNode().set("schemas", schemaInfoList);
  }

  protected void updateSchemaInformation(JsonNode mergedSchema,
                                         ArrayNode messagesMatched,
                                         ArrayNode schemaInformationList) {
    ObjectNode schemaElement = mapper.createObjectNode();
    try {
      schemaElement.set("schema", convertToFormat(mergedSchema, "Schema"));
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Unable generate schema for %s", mergedSchema), e);
    }
    schemaElement.set("messagesMatched", DeriveSchemaUtils.sortJsonArrayList(messagesMatched));
    schemaInformationList.add(schemaElement);
  }

  protected void getSingleDataType(ObjectNode mergedArray, ArrayNode items) {
    if (items.size() > 1) {
      throw new IllegalArgumentException(String.format("Found multiple data types: %s", items));
    } else if (items.size() == 1) {
      mergedArray.set("items", items.get(0));
    } else {
      // No items found, setting items as null
      mergedArray.set("items", getNullSchema());
    }
  }

  protected abstract JsonNode convertToFormat(JsonNode schema, String name);

  protected abstract ObjectNode mergeMultipleDataTypes(ObjectNode mergedArray,
                                                       List<JsonNode> primitives,
                                                       List<JsonNode> records,
                                                       List<JsonNode> arrays,
                                                       boolean check2dArray);

  protected abstract ArrayNode mergeMultipleMessages(List<JsonNode> uniqueSchemas,
                                                     Map<JsonNode, ArrayNode> schemaToIndex);

}
