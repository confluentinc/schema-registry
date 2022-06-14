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

import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema.getSortedKeys;
import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema.mapper;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Utility class for merging records and arrays in JSON.
 */
public final class MergeJsonUtils {

  private static void fillLists(ObjectNode element, ArrayList<String> items,
                                ArrayList<ObjectNode> records,
                                ArrayList<ObjectNode> arrays) {

    if (element.get("type").asText().equals("object")) {
      records.add(element);
    } else if (element.get("type").asText().equals("array")) {
      arrays.add(element);
    } else if (!items.contains(element.toString())) {
      items.add(element.toString());
    }
  }

  /**
   * The data type of each array is merged together using oneOf.
   *
   * @param arrayList List of array schemas
   * @return Merged Array
   */
  public static ObjectNode mergeArrays(ArrayList<ObjectNode> arrayList)
      throws JsonProcessingException {

    ObjectNode mergedArray = mapper.createObjectNode();
    mergedArray.put("type", "array");

    ArrayList<String> items = new ArrayList<>();
    ArrayList<ObjectNode> records = new ArrayList<>();
    ArrayList<ObjectNode> arrays = new ArrayList<>();

    for (ObjectNode array : arrayList) {

      ObjectNode field = (ObjectNode) array.get("items");
      if (field.isEmpty()) {
        continue;
      }

      if (field.has("oneOf")) {
        ArrayNode elements = (ArrayNode) field.get("oneOf");
        for (Object element : elements) {
          fillLists((ObjectNode) element, items, records, arrays);
        }
      } else {
        fillLists(field, items, records, arrays);
      }

    }

    ArrayNode jsonItems = mapper.createArrayNode();
    for (String item : items) {
      jsonItems.add(mapper.readTree(item));
    }

    if (records.size() > 0) {
      ObjectNode mergedRecords = mergeRecords(records);
      jsonItems.add(mergedRecords);
    }

    if (arrays.size() > 0) {
      ObjectNode mergedArrays = mergeArrays(arrays);
      jsonItems.add(mergedArrays);
    }

    if (jsonItems.size() > 1) {
      ObjectNode oneOfDataType = mapper.createObjectNode();
      oneOfDataType.set("oneOf", jsonItems);
      mergedArray.set("items", oneOfDataType);
    } else if (jsonItems.size() == 1) {
      mergedArray.set("items", jsonItems.get(0));
    } else {
      mergedArray.set("items", mapper.createObjectNode());
    }

    return mergedArray;
  }

  /**
   * The fields of each record are merged together into one record.
   *
   * @param recordList list of record schemas
   * @return Merged Record
   */
  public static ObjectNode mergeRecords(ArrayList<ObjectNode> recordList)
      throws JsonProcessingException {

    ObjectNode mergedRecord = mapper.createObjectNode();
    mergedRecord.put("type", "object");
    ObjectNode properties = mapper.createObjectNode();

    for (ObjectNode record : recordList) {

      ObjectNode fields = (ObjectNode) record.get("properties");
      for (String key : getSortedKeys(fields)) {

        if (!properties.has(key)) {
          properties.set(key, fields.get(key));
        } else {

          ObjectNode existingField = (ObjectNode) properties.get(key);
          ObjectNode newField = (ObjectNode) fields.get(key);

          if (existingField.has("type")) {

            if (existingField.get("type").asText().equals("object")
                && newField.get("type").asText().equals("object")) {

              ObjectNode mergedRecordRecursive = mergeRecords(
                  new ArrayList<>(Arrays.asList(existingField, newField)));
              properties.set(key, mergedRecordRecursive);
              continue;

            } else if (existingField.get("type").asText().equals("array")
                && newField.get("type").asText().equals("array")) {

              ObjectNode mergedArray = mergeArrays(
                  new ArrayList<>(Arrays.asList(existingField, newField)));
              properties.set(key, mergedArray);
              continue;

            }
          }

          // One is of Primitive type or oneOf
          if (!existingField.equals(newField)) {
            mergePrimitiveTypes(properties, key, fields);
          }
        }
      }
    }

    mergedRecord.set("properties", properties);
    return mergedRecord;
  }

  private static void fillMergedElements(ArrayList<String> mergedElements, ObjectNode schema) {

    ArrayNode oneOf = (ArrayNode) schema.get("oneOf");
    for (JsonNode obj : oneOf) {
      String objectString = obj.toString();
      if (!mergedElements.contains(objectString)) {
        mergedElements.add(objectString);
      }
    }
  }

  private static void fillElements(ArrayList<String> mergedElements, ObjectNode schema) {

    if (schema.has("oneOf")) {
      fillMergedElements(mergedElements, schema);
    } else if (!mergedElements.contains(schema.toString())) {
      mergedElements.add(schema.toString());
    }
  }

  /**
   * Merge oneOf elements and primitive elements together.
   *
   * @param properties ObjectNode where changes are stored
   * @param key        name of the property
   * @param fields     ObjectNode whose elements are used
   */
  private static void mergePrimitiveTypes(ObjectNode properties, String key, ObjectNode fields)
      throws JsonProcessingException {

    ObjectNode existingSchema = (ObjectNode) properties.get(key);
    ObjectNode newSchema = (ObjectNode) fields.get(key);
    ArrayList<String> mergedElements = new ArrayList<>();

    fillElements(mergedElements, existingSchema);
    fillElements(mergedElements, newSchema);

    ArrayNode jsonItems = mapper.createArrayNode();
    for (String mergedElement : mergedElements) {
      jsonItems.add(mapper.readTree(mergedElement));
    }

    if (jsonItems.size() > 1) {
      ObjectNode toPut = mapper.createObjectNode();
      toPut.set("oneOf", jsonItems);
      properties.set(key, toPut);
    } else {
      properties.set(key, jsonItems.get(0));
    }
  }

}
