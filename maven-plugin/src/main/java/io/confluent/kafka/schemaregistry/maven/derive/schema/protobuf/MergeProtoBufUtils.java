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

package io.confluent.kafka.schemaregistry.maven.derive.schema.protobuf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema;
import io.confluent.kafka.schemaregistry.maven.derive.schema.avro.DeriveAvroSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.stream.Collectors;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema.getSortedKeys;
import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema.mapper;

public final class MergeProtoBufUtils {

  private static final Logger logger = LoggerFactory.getLogger(MergeProtoBufUtils.class);

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
  public static void mergeRecordsInsideArray(ArrayList<ObjectNode> schemaList,
                                             boolean strictCheck) throws JsonProcessingException {

    ArrayList<ObjectNode> records = new ArrayList<>();
    ArrayList<Integer> recordsIndices = new ArrayList<>();

    for (int i = 0; i < schemaList.size(); i++) {
      ObjectNode field = schemaList.get(i);
      if (field.has("__type") && field.get("__type").asText().equals("record")) {
        records.add(field);
        recordsIndices.add(i);
      }
    }

    if (records.size() > 0) {
      ObjectNode mergedRecords = mergeRecords(records, strictCheck, true);
      for (Integer index : recordsIndices) {
        schemaList.set(index, mergedRecords);
      }
    }
  }

  /**
   * All the fields are merged together in one record.
   * <p>For lenient check in case of conflict, the highest occurring type is chosen.
   * In case of strict check, IllegalArgumentException is raised</p>
   *
   * @param recordList  List of records, where each record is a ObjectNode
   * @param strictCheck flag to specify strict check
   * @return Merged Record
   * @throws IllegalArgumentException thrown for strict check if a field with same name
   *                                  has different types in records
   */
  public static ObjectNode mergeRecords(ArrayList<ObjectNode> recordList, boolean strictCheck,
                                        boolean listError)
      throws IllegalArgumentException, JsonProcessingException {

    ObjectNode mergedRecord = mapper.createObjectNode();
    mergedRecord.put("__type", "record");
    mergedRecord.put("name", recordList.get(0).get("name").asText());
    mergedRecord.put("type", "record");

    ObjectNode properties = mapper.createObjectNode();
    Map<String, ArrayList<String>> schemaList = new HashMap<>();

    for (ObjectNode record : recordList) {

      ArrayNode fields = (ArrayNode) record.get("fields");

      for (Object field : fields) {

        String key = ((ObjectNode) field).get("name").asText();
        JsonNode newField = ((ObjectNode) field).get("type");

        ArrayList<String> elementTypes = schemaList.getOrDefault(key, new ArrayList<>());
        if (newField instanceof TextNode) {
          elementTypes.add(newField.asText());
        } else {
          elementTypes.add(newField.toString());
        }
        schemaList.put(key, elementTypes);

        if (!properties.has(key)) {
          properties.set(key, newField);
        } else {

          // field with same name exists
          Object existingField = properties.get(key);

          // If field has same the structure, no further checking
          if (existingField.equals(newField)) {
            continue;
          }

          // Field with a different structure raises error
          // only exception is field itself is a record
          if (existingField instanceof ObjectNode && newField instanceof ObjectNode) {
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

    mergedRecord.set("fields", getModeForFields(properties, schemaList, strictCheck, listError));
    return mergedRecord;
  }

  private static void mergeRecord(Object existingField, Object newField,
                                  ObjectNode properties, String key, boolean strictCheck,
                                  boolean listError) throws JsonProcessingException {

    ObjectNode existingFieldObject = (ObjectNode) existingField;
    ObjectNode newFieldObject = (ObjectNode) newField;

    if (existingFieldObject.equals(newFieldObject)) {
      return;
    }

    if (existingFieldObject.get("__type").asText().equals("record")
        && newFieldObject.get("__type").asText().equals("record")) {
      ObjectNode mergedRecord = mergeRecords(new ArrayList<>(
              Arrays.asList(existingFieldObject, newFieldObject)),
          strictCheck, listError);
      properties.set(key, mergedRecord);
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
   * @param properties  ObjectNode where changes are stored
   * @param schemaList  list of schemas
   * @param strictCheck flag to specify strict check
   * @return JSONArray as the schema chosen
   */
  private static JsonNode getModeForFields(ObjectNode properties, Map<String,
      ArrayList<String>> schemaList, boolean strictCheck, boolean listError) {

    ArrayNode fields = mapper.createArrayNode();

    for (String key : getSortedKeys(properties)) {

      ObjectNode field = mapper.createObjectNode();
      if (properties.get(key) instanceof ObjectNode) {
        field.set("__type", (properties.get(key)).get("__type"));
      }

      field.put("name", key);
      if (strictCheck) {
        field.set("type", properties.get(key));
      } else {
        int modeIndex = getMode(schemaList.get(key));

        // If mode is of type record, it may be possible to merge with other records
        // Return merged record in this else most occurring data type

        try {
          ObjectNode obj = (ObjectNode) mapper.readTree(schemaList.get(key).get(modeIndex));
          if (obj.get("__type").asText().equals("record")) {
            field.set("type", tryAndMergeStrict(schemaList.get(key)).getSchemas().get(0));
          } else {
            field.set("type", obj);
          }
        } catch (JsonProcessingException e) {
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

      fields.add(field);
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
   * @param schemaList List of schema, each element is ObjectNode
   * @return Object of type MapAndArray containing merged schemas
   *          and information of how many each messages each schema matches
   * @throws JsonProcessingException thrown if element not in JSON format
   */
  public static MapAndArray tryAndMergeStrictFromList(ArrayList<ObjectNode> schemaList,
                                                      List<List<Integer>> schemaToMessagesInfoInput)
      throws JsonProcessingException {

    ArrayList<ObjectNode> mergedSchemas = new ArrayList<>();
    List<List<Integer>> schemaToMessagesInfo = new ArrayList<>();
    for (int i = 0; i < schemaList.size(); i++) {
      schemaToMessagesInfo.add(new ArrayList<>());
    }

    for (int i = 0; i < schemaList.size(); i++) {

      ObjectNode currentSchema = schemaList.get(i);
      for (int j = 0; j < schemaList.size(); j++) {

        ObjectNode schemaToMatch = schemaList.get(j);
        try {
          currentSchema = MergeProtoBufUtils.mergeRecords(
              new ArrayList<>(Arrays.asList(currentSchema, schemaToMatch)), true, false);

          if (schemaToMessagesInfoInput == null) {
            schemaToMessagesInfo.get(i).add(j);
          } else {
            schemaToMessagesInfo.get(i).addAll(schemaToMessagesInfoInput.get(j));
          }

        } catch (IllegalArgumentException ignored) {
          continue;
        }
      }

      mergedSchemas.add(currentSchema);
    }

    ArrayList<ObjectNode> uniqueList = DeriveSchema.getUnique(mergedSchemas);
    List<List<Integer>> schemaToMessagesInfo2 = MergeProtoBufUtils.getUniqueWithMessageInfo(
        mergedSchemas, uniqueList, schemaToMessagesInfo);

    Comparator<ObjectNode> comparator
        = Comparator.comparing(o -> schemaToMessagesInfo.get(mergedSchemas.indexOf(o)).size());

    Comparator<List<Integer>> comparatorList = Comparator.comparing(List::size);
    uniqueList.sort(comparator.reversed());
    schemaToMessagesInfo2.sort(comparatorList.reversed());
    return new MapAndArray(uniqueList, schemaToMessagesInfo2);
  }

  /**
   * Generates ObjectNode from string, if possible and calls tryAndMergeStrictFromList.
   *
   * @param schemaStrings List of schema, each element is String
   * @return Object of type MapAndArray containing merged schemas
   *          and information of how many each messages each schema matches
   * @throws JsonProcessingException thrown if element not in JSON format
   */
  public static MapAndArray tryAndMergeStrict(ArrayList<String> schemaStrings)
      throws JsonProcessingException {

    ArrayList<ObjectNode> schemaList = new ArrayList<>();
    for (String schemaString : schemaStrings) {
      try {
        schemaList.add((ObjectNode) mapper.readTree(schemaString));
      } catch (Exception ignored) {
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

    int maxFrequency = 0;
    int indexOfMax = 0;

    for (int i = 0; i < list.size(); i++) {
      int curr = Collections.frequency(list, list.get(i));
      if (maxFrequency < curr) {
        maxFrequency = curr;
        indexOfMax = i;
      }
    }

    return indexOfMax;
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
  public static List<List<Integer>> getUniqueWithMessageInfo(ArrayList<ObjectNode> schemaList,
                                                             ArrayList<ObjectNode> uniqueList,
                                                             List<List<Integer>> schemaToMessages) {

    List<List<Integer>> schemaToMessagesForUnique = new ArrayList<>();
    for (int i = 0; i < uniqueList.size(); i++) {
      schemaToMessagesForUnique.add(new ArrayList<>());
    }

    for (int i = 0; i < schemaList.size(); i++) {
      for (int j = 0; j < uniqueList.size(); j++) {

        if (schemaList.get(i).equals(uniqueList.get(j))) {

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

