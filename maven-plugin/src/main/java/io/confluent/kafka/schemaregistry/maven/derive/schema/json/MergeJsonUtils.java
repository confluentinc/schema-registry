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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchemaUtils.mapper;
import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchemaUtils.sortObjectNode;
import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchemaUtils.getUnique;
import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchemaUtils.getSortedKeys;
import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchemaUtils.sortJsonArrayList;

public final class MergeJsonUtils {

  static void groupItems(ObjectNode element,
                         ArrayList<ObjectNode> items,
                         ArrayList<ObjectNode> records,
                         ArrayList<ObjectNode> arrays) {
    if (element.isEmpty() || items.contains(element)) {
      return;
    }
    // If element is oneOf type, add all elements inside oneOf
    if (element.has("oneOf")) {
      ArrayNode elements = (ArrayNode) element.get("oneOf");
      for (JsonNode oneOfElement : elements) {
        groupItems((ObjectNode) oneOfElement, items, records, arrays);
      }
      return;
    }

    if (element.has("type")) {
      String typeOfElement = element.get("type").asText();
      if (typeOfElement.equals("object")) {
        records.add(element);
      } else if (typeOfElement.equals("array")) {
        arrays.add(element);
      } else {
        items.add(element);
      }
    } else {
      items.add(element);
    }
  }

  public static ObjectNode mergeArrays(ArrayList<ObjectNode> arrayList, boolean useItems) {
    // Merging different field types into one type
    ObjectNode mergedArray = mapper.createObjectNode();
    mergedArray.put("type", "array");

    ArrayList<ObjectNode> primitives = new ArrayList<>();
    ArrayList<ObjectNode> records = new ArrayList<>();
    ArrayList<ObjectNode> arrays = new ArrayList<>();

    // Group items of array into record, array and primitive types
    for (ObjectNode array : getUnique(arrayList)) {
      if (!useItems) {
        groupItems(array, primitives, records, arrays);
      } else {
        groupItems((ObjectNode) array.get("items"), primitives, records, arrays);
      }
    }

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

  public static ObjectNode mergeRecords(ArrayList<ObjectNode> recordList) {
    // Merge all fields in all the records together into one record
    ObjectNode mergedRecord = mapper.createObjectNode();
    mergedRecord.put("type", "object");
    ObjectNode properties = mapper.createObjectNode();
    HashMap<String, ArrayList<ObjectNode>> fieldToItsType = new HashMap<>();

    /*
      Loop through every record group schemas by field name
      Then for each field treat the list of schemas as array and try to merge
    */
    for (ObjectNode record : recordList) {
      ObjectNode fields = (ObjectNode) record.get("properties");
      for (String fieldName : getSortedKeys(fields)) {
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

    mergedRecord.set("properties", sortObjectNode(properties));
    return mergedRecord;
  }
}
