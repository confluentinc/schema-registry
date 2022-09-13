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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema.mapper;
import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema.sortJsonArrayList;
import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema.getSortedKeys;
import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema.sortObjectNode;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.json.DeriveJsonSchema.groupItems;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public final class MergeJsonUtils {

  public static ArrayList<ObjectNode> convertItemsToArrayItems(ArrayList<ObjectNode> uniqueList) {
    // Converting each item to type array and setting items to original value
    ArrayList<ObjectNode> uniqueListAsArray = new ArrayList<>();
    for (ObjectNode uniqueSchema : uniqueList) {
      ObjectNode arrayTypeSchema = mapper.createObjectNode();
      arrayTypeSchema.put("type", "array");
      arrayTypeSchema.set("items", uniqueSchema);
      uniqueListAsArray.add(arrayTypeSchema);
    }
    return uniqueListAsArray;
  }

  public static ObjectNode mergeArrays(ArrayList<ObjectNode> arrayList) {
    // Merging different field types into one type
    ObjectNode mergedArray = mapper.createObjectNode();
    mergedArray.put("type", "array");

    ArrayList<ObjectNode> primitives = new ArrayList<>();
    ArrayList<ObjectNode> records = new ArrayList<>();
    ArrayList<ObjectNode> arrays = new ArrayList<>();

    // Group items of array into record, array and primitive types
    for (ObjectNode array : DeriveSchema.getUnique(arrayList)) {
      ObjectNode itemsOfArray = (ObjectNode) array.get("items");
      if (itemsOfArray.has("oneOf")) {
        ArrayNode elements = (ArrayNode) itemsOfArray.get("oneOf");
        for (Object element : elements) {
          groupItems((ObjectNode) element, primitives, records, arrays);
        }
      } else if (!itemsOfArray.isEmpty()) {
        groupItems(itemsOfArray, primitives, records, arrays);
      }
    }

    ArrayNode jsonItems = mapper.createArrayNode();
    // Adding primitive types to items' list
    for (ObjectNode item : primitives) {
      jsonItems.add(item);
    }

    // Merge records if there is more than 1 record
    if (records.size() > 0) {
      ObjectNode mergedRecords = mergeRecords(records);
      jsonItems.add(mergedRecords);
    }

    // Merge arrays if there is more than 1 array
    if (arrays.size() > 0) {
      ObjectNode mergedArrays = mergeArrays(arrays);
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
      ObjectNode items = mergeArrays(convertItemsToArrayItems(fieldsType));
      properties.set(entry.getKey(), items.get("items"));
    }

    mergedRecord.set("properties", sortObjectNode(properties));
    return mergedRecord;
  }
}
