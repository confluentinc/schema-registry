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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveAvroSchema;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import java.util.stream.Collectors;

/**
 * Utility class that has functions for merging records and numbers in Avro and ProtoBuf.
 */

public final class MergeAvroProtoBufUtils {

  private static final Logger logger = LoggerFactory.getLogger(MergeAvroProtoBufUtils.class);

  private static String getErrorMessage(String key, Object existingField, Object newField) {
    String errorMessage = "Two fields exist with same name %s"
        + " but different structure. Structure 1: %s and Structure 2: %s";
    return String.format(errorMessage, key, existingField, newField);
  }

  /**
   * Function to find and merge all records present in array.
   *
   * @param schemaList  list of schemas to merge
   * @param strictCheck flag to specify strict check
   */
  public static void mergeRecordsInsideArray(ArrayList<JSONObject> schemaList,
                                             boolean strictCheck) throws JsonProcessingException {

    ArrayList<JSONObject> records = new ArrayList<>();
    ArrayList<Integer> recordsIndices = new ArrayList<>();

    for (int i = 0; i < schemaList.size(); i++) {
      JSONObject field = schemaList.get(i);
      if (field.has("__type") && field.get("__type").equals("record")) {
        records.add(field);
        recordsIndices.add(i);
      }
    }

    if (records.size() > 0) {
      JSONObject mergedRecords = mergeRecords(records, strictCheck, true);
      for (Integer index : recordsIndices) {
        schemaList.set(index, mergedRecords);
      }
    }

  }

  /**
   * Merge together fields of type int, long, float and double.
   *
   * @param schemaList list of schemas to merge
   */
  public static void mergeNumberTypes(ArrayList<JSONObject> schemaList, boolean matchAll) {

    if (schemaList.size() == 0 || schemaList.size() == 1) {
      return;
    }

    /*
    Using O(n*n) version for 2 reasons over O(n) version checking only with 1st schema
    1. A field is absent in the first schema, hence never check
    2. A field is present in the first schema, but of type non-Number
    but rest of the messages are of number type.
     */

    if (matchAll) {
      for (JSONObject curr : schemaList) {
        for (JSONObject starting : schemaList) {
          MergeNumberUtils.adjustNumberTypes(starting, curr);
        }
      }
    } else {

      for (JSONObject curr : schemaList) {
        MergeNumberUtils.adjustNumberTypes(schemaList.get(0), curr);
      }

      for (JSONObject curr : schemaList) {
        MergeNumberUtils.adjustNumberTypes(schemaList.get(0), curr);
      }

    }

  }

  /**
   * All the fields are merged together in one record.
   * <p>For lenient check in case of conflict, the highest occurring type is chosen.
   * In case of strict check, IllegalArgumentException is raised</p>
   *
   * @param recordList  List of records, where each record is a JSONObject
   * @param strictCheck flag to specify strict check
   * @return Merged Record
   * @throws IllegalArgumentException thrown for strict check if a field with same name
   *                                  has different types in records
   */
  public static JSONObject mergeRecords(ArrayList<JSONObject> recordList, boolean strictCheck,
                                        boolean listError)
      throws IllegalArgumentException, JsonProcessingException {

    JSONObject ans = new JSONObject();
    ans.put("type", "record");
    ans.put("__type", "record");
    ans.put("name", recordList.get(0).get("name"));

    JSONObject properties = new JSONObject();
    Map<String, ArrayList<String>> schemaList = new HashMap<>();

    for (JSONObject record : recordList) {

      JSONArray fields = record.getJSONArray("fields");

      for (Object field : fields) {

        String key = ((JSONObject) field).get("name").toString();
        Object newField = ((JSONObject) field).get("type");

        ArrayList<String> elementTypes = schemaList.getOrDefault(key, new ArrayList<>());
        elementTypes.add(newField.toString());
        schemaList.put(key, elementTypes);

        if (!properties.has(key)) {
          properties.put(key, newField);
        } else {

          // field with same name exists
          Object existingField = properties.get(key);

          // If field has same the structure, no further checking
          if (existingField.equals(newField)) {
            continue;
          }

          // Field with a different structure raises error
          // only exception is field itself is a record
          if (existingField instanceof JSONObject && newField instanceof JSONObject) {
            mergeRecord(existingField, newField, properties, key, strictCheck, listError);
          } else if (strictCheck) {
            if (listError) {
              logger.error(getErrorMessage(key, existingField, newField));
            }
            throw new IllegalArgumentException(getErrorMessage(key, existingField, newField));
          }

        }
      }

    }

    ans.put("fields", getModeForFields(properties, schemaList, strictCheck, listError));
    return ans;
  }


  private static void mergeRecord(Object existingField, Object newField,
                                  JSONObject properties, String key, boolean strictCheck,
                                  boolean listError) throws JsonProcessingException {

    JSONObject existingFieldObject = (JSONObject) existingField;
    JSONObject newFieldObject = (JSONObject) newField;

    if (existingFieldObject.similar(newFieldObject)) {
      return;
    }

    if (existingFieldObject.get("__type").equals("record")
        && newFieldObject.get("__type").equals("record")) {
      JSONObject mergedRecord = mergeRecords(new ArrayList<>(
              Arrays.asList(existingFieldObject, newFieldObject)),
          strictCheck, listError);
      properties.put(key, mergedRecord);
    } else if (strictCheck) {

      if (listError) {
        logger.error(getErrorMessage(key, existingField, newField));
      }
      throw new IllegalArgumentException(getErrorMessage(key, existingField, newField));

    }

  }

  /**
   * For lenient check, if there is conflict the highest occurring type is chosen and
   * comments are added for the same.
   *
   * @param properties  JSONObject where changes are stored
   * @param schemaList  list of schemas
   * @param strictCheck flag to specify strict check
   * @return JSONArray as the schema chosen
   */

  private static JSONArray getModeForFields(JSONObject properties, Map<String,
      ArrayList<String>> schemaList, boolean strictCheck, boolean listError)
      throws JsonProcessingException {

    JSONArray fields = new JSONArray();
    TreeSet<String> keySet = new TreeSet<>(properties.keySet());
    for (String key : keySet) {

      JSONObject field = new JSONObject();
      field.put("name", key);

      if (strictCheck) {
        field.put("type", properties.get(key));
      } else {

        int modeIndex = getMode(schemaList.get(key));

        // If mode is of type record, it may be possible to merge with other records
        // Return merged record in this else most occurring data type

        try {
          JSONObject obj = new JSONObject(schemaList.get(key).get(modeIndex));
          if (obj.get("__type").equals("record")) {
            field.put("type", tryAndMergeStrict(schemaList.get(key)).getSchemas().get(0));
          } else {
            field.put("type", obj);
          }
        } catch (JSONException e) {
          field.put("type", schemaList.get(key).get(modeIndex));
        }

        int freq = Collections.frequency(schemaList.get(key), schemaList.get(key).get(modeIndex));
        if (freq != schemaList.get(key).size()) {
          if (listError) {
            String errorString = String.format("Message %d: Warning! All elements "
                    + "should be of same type. Choosing most frequent element for schema",
                DeriveAvroSchema.getCurrentMessage());
            logger.warn(errorString);
          }
        }

      }

      if (properties.get(key) instanceof JSONObject) {
        field.put("__type", (properties.getJSONObject(key)).get("__type"));
      }
      fields.put(field);

    }

    return fields;
  }

  /**
   * Picks a record 'i' and tries to merge with the rest of the n-1 elements.
   * This process is repeated for all elements.
   *
   * <p>
   * Matching of strict type, in case of any conflicts, error is returned
   * and inferred as schemas cannot be merged.
   * </p>
   *
   * @param schemaList List of schema, each element is JSONObject
   * @return Object of type MapAndArray containing merged schemas
   *          and information of how many each messages each schema matches
   * @throws JsonProcessingException thrown if element not in JSON format
   */

  public static MapAndArray tryAndMergeStrictFromList(ArrayList<JSONObject> schemaList,
                                                      List<List<Integer>> schemaToMessagesInfoInput)
      throws JsonProcessingException {

    ArrayList<JSONObject> mergedSchemas = new ArrayList<>();
    List<List<Integer>> schemaToMessagesInfo = new ArrayList<>();
    for (int i = 0; i < schemaList.size(); i++) {
      schemaToMessagesInfo.add(new ArrayList<>());
    }

    for (int i = 0; i < schemaList.size(); i++) {

      JSONObject curr = schemaList.get(i);
      for (int j = 0; j < schemaList.size(); j++) {

        JSONObject toMatch = schemaList.get(j);

        try {
          curr = MergeAvroProtoBufUtils.mergeRecords(
              new ArrayList<>(Arrays.asList(curr, toMatch)), true, false);

          if (schemaToMessagesInfoInput == null) {
            schemaToMessagesInfo.get(i).add(j);
          } else {
            schemaToMessagesInfo.get(i).addAll(schemaToMessagesInfoInput.get(j));
          }

        } catch (IllegalArgumentException ignored) {
          continue;
        }
      }

      mergedSchemas.add(curr);

    }

    ArrayList<JSONObject> uniqueList = MergeJsonUtils.getUnique(mergedSchemas);
    List<List<Integer>> schemaToMessagesInfo2 = MergeAvroProtoBufUtils.getUniqueWithMessageInfo(
        mergedSchemas, uniqueList, schemaToMessagesInfo);

    Comparator<JSONObject> comparator
        = Comparator.comparing(o -> schemaToMessagesInfo.get(mergedSchemas.indexOf(o)).size());

    Comparator<List<Integer>> comparatorList = Comparator.comparing(List::size);

    uniqueList.sort(comparator.reversed());
    schemaToMessagesInfo2.sort(comparatorList.reversed());

    return new MapAndArray(uniqueList, schemaToMessagesInfo2);
  }

  /**
   * Generates JSONObjects from string, if possible and calls tryAndMergeStrictFromList.
   *
   * @param schemaStrings List of schema, each element is String
   * @return Object of type MapAndArray containing merged schemas
   *          and information of how many each messages each schema matches
   * @throws JsonProcessingException thrown if element not in JSON format
   */
  public static MapAndArray tryAndMergeStrict(ArrayList<String> schemaStrings)
      throws JsonProcessingException {

    ArrayList<JSONObject> schemaList = new ArrayList<>();
    for (String schemaString : schemaStrings) {
      try {
        schemaList.add(new JSONObject(schemaString));
      } catch (JSONException ignored) {
        continue;
      }
    }
    return tryAndMergeStrictFromList(schemaList, null);
  }

  /**
   * Returns mode in the list.
   *
   * @param list - list of schemas
   * @return mode of strings
   */
  public static int getMode(ArrayList<String> list) {

    int max = 0;
    int maxi = 0;

    for (int i = 0; i < list.size(); i++) {
      int curr = Collections.frequency(list, list.get(i));
      if (max < curr) {
        max = curr;
        maxi = i;
      }
    }

    return maxi;
  }

  /**
   * schemaToMessages provides information on which messages each schema matches,
   * the same information is populated for unique schemas generated
   *
   * @param schemaList       List with schemas
   * @param uniqueList       List with only unique schemas
   * @param schemaToMessages List with information on which messages each schema matches,
   *                         if no prior unique schemas are generated this value can be left null
   * @return List with information on which messages each unique schema matches
   */
  public static List<List<Integer>> getUniqueWithMessageInfo(ArrayList<JSONObject> schemaList,
                                                             ArrayList<JSONObject> uniqueList,
                                                             List<List<Integer>> schemaToMessages) {

    List<List<Integer>> schemaToMessagesForUnique = new ArrayList<>();
    for (int i = 0; i < uniqueList.size(); i++) {
      schemaToMessagesForUnique.add(new ArrayList<>());
    }

    for (int i = 0; i < schemaList.size(); i++) {
      for (int j = 0; j < uniqueList.size(); j++) {

        if (schemaList.get(i).similar(uniqueList.get(j))) {

          if (schemaToMessages == null) {
            schemaToMessagesForUnique.get(j).add(i);
          } else {
            schemaToMessagesForUnique.get(j).addAll(schemaToMessages.get(i));
          }
          break;

        }

      }
    }

    for (int i = 0; i < uniqueList.size(); i++) {
      schemaToMessagesForUnique.set(i, schemaToMessagesForUnique.get(i).stream().distinct()
          .collect(Collectors.toList()));
    }

    return schemaToMessagesForUnique;

  }

}

