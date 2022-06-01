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

package io.confluent.kafka.schemaregistry.maven.derive.schema.utils;

import com.google.common.base.Equivalence;
import org.jetbrains.annotations.NotNull;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class that has functions for merging records and arrays in JSON.
 */

public final class JsonUtils {

  /**
   * Get unique JSONObjects from a list of JSONObjects and remove duplicates.
   * <p>
   * Default set in Java and 'equals' method of JSONObjects doesn't work as desired.
   * JSONObjects with the same structure are duplicates.
   * Using custom comparator 'Equivalence', set is created and converted to array for final answer.
   * </p>
   *
   * @param schemas List of JSONObjects
   * @return Unique set of JSONObjects
   */
  public static ArrayList<JSONObject> getUnique(ArrayList<JSONObject> schemas) {

    Equivalence<JSONObject> equivalence = new Equivalence<JSONObject>() {
      @Override
      protected boolean doEquivalent(JSONObject a, @NotNull JSONObject b) {
        return a.similar(b);
      }

      @Override
      protected int doHash(@NotNull JSONObject jsonObject) {
        return 0;
      }

    };

    Set<Equivalence.Wrapper<JSONObject>> setWithWrappedObjects = schemas.stream()
        .map(equivalence::wrap)
        .collect(Collectors.toSet());

    ArrayList<JSONObject> ans = new ArrayList<>();

    for (Equivalence.Wrapper<JSONObject> jsonObjectWrapper : setWithWrappedObjects) {
      JSONObject jsonObject = jsonObjectWrapper.get();
      if (jsonObject != null && !jsonObject.isEmpty()) {
        ans.add(jsonObjectWrapper.get());
      }
    }

    return ans;
  }

  private static void fillLists(JSONObject element, ArrayList<String> items,
                                ArrayList<JSONObject> records,
                                ArrayList<JSONObject> arrays) {

    if (element.get("type").equals("object")) {
      records.add(element);
    } else if (element.get("type").equals("array")) {
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
  public static JSONObject mergeArrays(ArrayList<JSONObject> arrayList) {

    JSONObject ans = new JSONObject();
    ans.put("type", "array");

    ArrayList<String> items = new ArrayList<>();
    ArrayList<JSONObject> records = new ArrayList<>();
    ArrayList<JSONObject> arrays = new ArrayList<>();

    for (JSONObject arr : arrayList) {

      JSONObject field = arr.getJSONObject("items");
      if (field.isEmpty()) {
        continue;
      }

      if (field.has("oneOf")) {
        JSONArray elements = field.getJSONArray("oneOf");
        for (Object element : elements) {
          fillLists((JSONObject) element, items, records, arrays);
        }
      } else {
        fillLists(field, items, records, arrays);
      }

    }

    JSONArray jsonItems = new JSONArray();
    for (String item : items) {
      jsonItems.put(new JSONObject(item));
    }
    if (records.size() > 0) {
      JSONObject mergedRecords = mergeRecords(records);
      jsonItems.put(mergedRecords);
    }

    if (arrays.size() > 0) {
      JSONObject mergedArrays = mergeArrays(arrays);
      jsonItems.put(mergedArrays);
    }

    if (jsonItems.length() > 1) {
      JSONObject oneOf = new JSONObject();
      oneOf.put("oneOf", jsonItems);
      ans.put("items", oneOf);
    } else if (jsonItems.length() == 1) {
      ans.put("items", jsonItems.get(0));
    } else {
      ans.put("items", new JSONObject());
    }


    return ans;
  }

  /**
   * The fields of each record are merged together into one record.
   *
   * @param recordList list of record schemas
   * @return Merged Record
   */
  public static JSONObject mergeRecords(ArrayList<JSONObject> recordList) {

    JSONObject ans = new JSONObject();
    ans.put("type", "object");

    JSONObject properties = new JSONObject();

    for (JSONObject record : recordList) {

      JSONObject fields = record.getJSONObject("properties");
      for (String key : fields.keySet()) {

        if (!properties.has(key)) {
          properties.put(key, fields.get(key));
        } else {

          JSONObject existingField = properties.getJSONObject(key);
          JSONObject newField = fields.getJSONObject(key);

          if (existingField.has("type")) {

            if (existingField.get("type").equals("object")
                && newField.get("type").equals("object")) {
              JSONObject x = mergeRecords(new ArrayList<>(Arrays.asList(existingField, newField)));
              properties.put(key, x);
              continue;
            } else if (existingField.get("type").equals("array")
                && newField.get("type").equals("array")) {
              JSONObject x = mergeArrays(new ArrayList<>(Arrays.asList(existingField, newField)));
              properties.put(key, x);
              continue;
            }

          }

          // One is of Primitive type or oneOf
          if (!existingField.similar(newField)) {
            mergePrimitiveTypes(properties, key, fields);
          }

        }

      }
    }

    ans.put("properties", properties);
    return ans;

  }

  /**
   * Helper function to put elements of oneOf in schema in mergedElements.
   *
   * @param mergedElements list of data types in oneOf
   * @param schema         schema whose elements are added to mergedElements
   */
  private static void fillMergedElements(ArrayList<String> mergedElements, JSONObject schema) {

    JSONArray oneOf = schema.getJSONArray("oneOf");
    for (Object obj : oneOf) {
      String jsonObject = obj.toString();
      if (!mergedElements.contains(jsonObject)) {
        mergedElements.add(jsonObject);
      }
    }

  }

  /**
   * Helper function to fill mergedElements from schema.
   *
   * @param mergedElements list of data types in oneOf
   * @param schema         schema whose elements are added to mergedElements
   */
  private static void fillElements(ArrayList<String> mergedElements, JSONObject schema) {

    if (schema.has("oneOf")) {
      fillMergedElements(mergedElements, schema);
    } else if (!mergedElements.contains(schema.toString())) {
      mergedElements.add(schema.toString());
    }

  }

  /**
   * Merge oneOf elements and primitive elements together.
   *
   * @param properties JSONObject where changes are stored
   * @param key        name of the property
   * @param fields     JSONObject whose elements are used
   */
  private static void mergePrimitiveTypes(JSONObject properties, String key, JSONObject fields) {

    JSONObject existingSchema = properties.getJSONObject(key);
    JSONObject newSchema = fields.getJSONObject(key);
    ArrayList<String> mergedElements = new ArrayList<>();

    fillElements(mergedElements, existingSchema);
    fillElements(mergedElements, newSchema);

    JSONArray jsonItems = new JSONArray();
    for (String mergedElement : mergedElements) {
      jsonItems.put(new JSONObject(mergedElement));
    }

    if (jsonItems.length() > 1) {
      JSONObject toPut = new JSONObject();
      toPut.put("oneOf", jsonItems);
      properties.put(key, toPut);
    } else {
      properties.put(key, jsonItems.get(0));
    }

  }

}
