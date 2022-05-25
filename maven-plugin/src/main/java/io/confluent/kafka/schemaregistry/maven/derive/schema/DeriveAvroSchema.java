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
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;


import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.maven.derive.schema.utils.MergeAvroProtoBufUtils;
import io.confluent.kafka.schemaregistry.maven.derive.schema.utils.MergeJsonUtils;
import io.confluent.kafka.schemaregistry.maven.derive.schema.utils.MergeUnionUtils;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class with functionality to derive schema from messages for avro and
 * generate structure for ProtoBuf.
 */
public class DeriveAvroSchema extends DeriveSchema {

  private static final Logger logger = LoggerFactory.getLogger(DeriveAvroSchema.class);
  static final Map<String, String> classToDataType = new HashMap<>();
  final boolean strictCheck;
  final boolean typeProtoBuf;
  private static final Gson gson = new Gson();
  private static int currentMessage = -1;

  public static int getCurrentMessage() {
    return currentMessage;
  }

  private static void setCurrentMessage(int currentMessage) {
    DeriveAvroSchema.currentMessage = currentMessage;
  }

  String errorMessageNoSchemasFound = "No strict schemas can be generated for the given messages. "
      + "Please try using the lenient version to generate a schema.";

  /**
   * Basic constructor to specify strict and schema type.
   *
   * @param isStrict     flag to specify strict check
   * @param typeProtoBuf flag to specify type ProtoBuf
   */
  public DeriveAvroSchema(boolean isStrict, boolean typeProtoBuf) {
    this.strictCheck = isStrict;
    this.typeProtoBuf = typeProtoBuf;
    fillMap();
  }

  /**
   * Basic constructor to specify strict and type by default assumed as avro.
   *
   * @param isStrict flag to specify strict check
   */
  public DeriveAvroSchema(boolean isStrict) {
    this.strictCheck = isStrict;
    this.typeProtoBuf = false;
    fillMap();
  }


  private void fillMap() {

    classToDataType.put("class java.lang.String", "string");
    classToDataType.put("class java.math.BigDecimal", "double");

    if (this.typeProtoBuf) {
      fillProtoBufMap();
    } else {
      fillAvroMap();
    }

  }

  private void fillProtoBufMap() {
    classToDataType.put("class java.math.BigInteger", "double");
    classToDataType.put("class java.lang.Integer", "int32");
    classToDataType.put("class java.lang.Long", "int64");
    classToDataType.put("class java.lang.Boolean", "bool");
    classToDataType.put("class org.json.JSONObject$Null", "google.protobuf.Any");
  }

  private void fillAvroMap() {
    classToDataType.put("class java.math.BigInteger", "double");
    classToDataType.put("class java.lang.Integer", "int");
    classToDataType.put("class java.lang.Long", "long");
    classToDataType.put("class java.lang.Boolean", "boolean");
    classToDataType.put("class org.json.JSONObject$Null", "null");
  }


  private void checkName(String name) {

    if (name.length() == 0) {
      throw new IllegalArgumentException(
          String.format("Message %d: Name of a field cannot be Empty.", currentMessage));
    }

    if (name.matches("\\d.*")) {
      throw new IllegalArgumentException(
          String.format("Message %d: Name of a field cannot begin with digit.", currentMessage));
    }

    if (typeProtoBuf && name.contains(".")) {
      throw new IllegalArgumentException(
          String.format("Message %d: Name of field cannot contain '.'", currentMessage));
    }

  }

  /**
   * Primitive schema of message is generated, if possible.
   *
   * @param field - message whose schema has to be found
   * @return JSONObject if type primitive else empty option
   */
  Optional<JSONObject> getPrimitiveSchema(Object field) throws IllegalArgumentException {

    String jsonInferredType;

    if (field == null) {
      jsonInferredType = "class org.json.JSONObject$Null";
    } else {
      jsonInferredType = field.getClass().toString();
    }

    if (jsonInferredType.equals("class java.math.BigInteger")) {
      if (this.strictCheck) {
        String errorMessage = String.format("Message %d: Numeric value %s out of range of long "
                + "(-9223372036854775808 9223372036854775807). "
                + "Change value to string type or change value to inside the range of long",
            currentMessage, field);
        logger.error(errorMessage);
        throw new IllegalArgumentException(errorMessage);
      } else {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("type", "double");
        logger.warn(String.format("Message %d: Value out of range of long. "
            + "Mapping to double", currentMessage));
        return Optional.of(jsonObject);
      }
    }

    if (classToDataType.containsKey(jsonInferredType)) {
      String schemaString = String.format("{\"type\" : \"%s\"}",
          classToDataType.get(jsonInferredType));
      return Optional.of(new JSONObject(schemaString));
    }

    return Optional.empty();
  }

  /**
   * Return a list of schemas as JSONObject, a schema corresponding to each message.
   * <p>
   * Checks if message is of primitive, array or record type
   * and makes further function call accordingly
   * </p>
   *
   * <p>
   * For multiple messages, if valid schema is not possible, the schema is ignored and
   * warning message is logged.
   * </p>
   *
   * @param messages         List of messages, each message is a JSONObject
   * @param name             Name of Element
   * @param multipleMessages Flag to denote multiple message
   * @return List of Schemas, one for each message
   * @throws JsonProcessingException  thrown if message not in JSON format
   * @throws IllegalArgumentException thrown if no messages can be generated for strict check
   */
  public ArrayList<JSONObject> getSchemaOfAllElements(List<Object> messages, String name,
                                                      boolean multipleMessages)
      throws JsonProcessingException, IllegalArgumentException {

    ArrayList<JSONObject> arr = new ArrayList<>();

    for (int i = 0; i < messages.size(); i++) {

      setCurrentMessage(i);
      Object message = messages.get(i);
      Optional<JSONObject> x = getPrimitiveSchema(message);

      try {

        if (x.isPresent()) {
          if (x.get().get("type").equals("null")) {
            x.get().put("name", name);
          }
          arr.add(x.get());
        } else if (isArrayType(message.getClass().toString())) {
          List<Object> l = getListFromArray(message);
          arr.add(getSchemaForArray(l, name));
        } else {
          JSONObject jsonObject = getMapFromObject(message);
          arr.add(getSchemaForRecord(jsonObject, name, false));
        }

      } catch (IllegalArgumentException e) {
        if (multipleMessages) {
          // Empty Object added to maintain correct index
          arr.add(new JSONObject());
          logger.warn(String.format("Message %d: cannot find Strict schema. "
              + "Hence, ignoring for multiple messages. %s %n", i, e.getMessage()));
        } else {
          logger.error(e.getMessage());
          throw new IllegalArgumentException(e.getMessage());
        }
      }

    }

    return arr;
  }

  public ArrayList<JSONObject> getSchemaOfAllElements(List<Object> list, String name)
      throws JsonProcessingException {
    return getSchemaOfAllElements(list, name, false);
  }

  /**
   * Returns highest occurring datatype. If all elements have same occurrences, first is returned
   * <p>
   * Prints warning in case there are multiple types of elements. Expected one data type
   * </p>
   *
   * @param schemaList    List of schemas as JSONObjects
   * @param schemaStrings List of schema as Strings
   * @param name          Name of Array
   */

  JSONObject getModeForArray(ArrayList<JSONObject> schemaList, ArrayList<String> schemaStrings,
                             String name) {

    int modeIndex = MergeAvroProtoBufUtils.getMode(schemaStrings);
    int freq = Collections.frequency(schemaStrings, schemaStrings.get(modeIndex));
    if (freq != schemaStrings.size()) {
      logger.warn(String.format("Message %d: All elements should be of same type for array '%s'. "
          + "Choosing most frequent element for schema", currentMessage, name));
    }
    return schemaList.get(modeIndex);

  }


  /**
   * Checking if all elements have the same structure for strict check.
   *
   * @param schemaStrings List of schema as Strings
   * @param name          Name of Array
   * @throws IllegalArgumentException thrown when elements are of different types
   */
  private void checkForSameStructure(ArrayList<String> schemaStrings, String name)
      throws IllegalArgumentException {

    for (int i = 0; i < schemaStrings.size(); i++) {
      if (!schemaStrings.get(0).equals(schemaStrings.get(i))) {

        Type mapType = new TypeToken<Map<String, Object>>() {
        }.getType();
        Map<String, Object> firstMap = gson.fromJson(schemaStrings.get(i), mapType);
        Map<String, Object> secondMap = gson.fromJson(schemaStrings.get(0), mapType);
        String diff = Maps.difference(firstMap, secondMap).toString().substring(30);

        String errorMessage = String.format("Message %d: Array '%s' should have "
            + "all elements of the same type. Element %d is different from  Element %d. "
            + "Difference is : %s", currentMessage, name, i, 0, diff);
        logger.error(errorMessage);
        throw new IllegalArgumentException(errorMessage);
      }
    }

  }

  private ArrayList<JSONObject> getUniqueList(ArrayList<JSONObject> schemaList) {

    /*
    To reduce computation, use only unique values for strict check
    Lenient check we need frequency of data type, hence reassign schemaList
    only if 1 value remains use unique
    */

    ArrayList<JSONObject> uniqueSchemaList = MergeJsonUtils.getUnique(schemaList);
    if (uniqueSchemaList.size() == 1 || strictCheck) {
      return uniqueSchemaList;
    }
    return schemaList;

  }

  /**
   * Returns information(schema and comments) about type for array.
   *
   * <p>Performs merging of number types, merging of records for ProtoBuf
   * and merging of unions for Avro.</p>
   * <p>For strict version, checks if all schemas are same otherwise throws error.
   * For lenient version, picks most occurring type in case of conflicts</p>
   *
   * @param messages         List of messages, each message is a JSONObject
   * @param name             name used for record and arrays
   * @param multipleMessages flag to specify multiple messages
   * @return map with information about schema
   * @throws JsonProcessingException  thrown if message not in JSON format
   * @throws IllegalArgumentException thrown if schema cannot be found in strict version
   */
  public JSONObject getDatatypeForArray(List<Object> messages,
                                        String name,
                                        boolean multipleMessages)
      throws JsonProcessingException, IllegalArgumentException {

    ArrayList<JSONObject> schemaList = getSchemaOfAllElements(messages,
        name, multipleMessages);

    schemaList = getUniqueList(schemaList);

    MergeAvroProtoBufUtils.mergeNumberTypes(schemaList, !strictCheck);
    schemaList = getUniqueList(schemaList);

    if (typeProtoBuf) {
      // Merge Records for ProtoBuf
      MergeAvroProtoBufUtils.mergeRecordsInsideArray(schemaList, strictCheck);
    } else {
      // Merge Unions for Avro
      MergeUnionUtils.mergeUnion(schemaList, !strictCheck);
    }

    ArrayList<String> schemaStrings = new ArrayList<>();
    for (JSONObject jsonObject : schemaList) {
      schemaStrings.add(jsonObject.toString());
    }

    if (!strictCheck) {
      // Lenient check choose highest occurring schema
      if (schemaList.size() > 0) {
        return getModeForArray(schemaList, schemaStrings, name);
      }
      return new JSONObject();
    }

    // Check if all schemas are same, else raise error
    checkForSameStructure(schemaStrings, name);
    if (schemaList.size() > 0) {
      return schemaList.get(0);
    }
    return new JSONObject();


  }


  public JSONObject getSchemaForArray(List<Object> field, String name)
      throws JsonProcessingException {
    return getSchemaForArray(field, name, false, false);
  }

  /**
   * Generates schema for array type.
   *
   * @param messages         List of messages, each message is a JSONObject
   * @param name             name assigned to array
   * @param calledAsField    flag to check if called as a field
   * @param multipleMessages flag to specify multiple messages
   * @return schema
   * @throws JsonProcessingException  thrown if message not in JSON format
   * @throws IllegalArgumentException thrown if schema cannot be found in strict version
   */

  public JSONObject getSchemaForArray(List<Object> messages, String name,
                                      boolean calledAsField, boolean multipleMessages)
      throws JsonProcessingException, IllegalArgumentException {

    JSONObject schema = new JSONObject();
    schema.put("name", name);

    if (typeProtoBuf) {
      schema.put("__type", "array");
    }

    if (!calledAsField) {
      schema.put("type", "array");
    }

    JSONObject elementSchema = getDatatypeForArray(messages, name, multipleMessages);

    if (!elementSchema.isEmpty()) {
      DeriveProtobufSchema.checkForArrayOfNull(typeProtoBuf, elementSchema);
    } else {
      elementSchema.put("type", new JSONArray());
    }

    if (!elementSchema.has("name")) {

      elementSchema.put("items", elementSchema.get("type"));
      elementSchema.put("type", "array");
      if (typeProtoBuf) {
        elementSchema.put("__type", "array");
      }

      if (calledAsField) {
        schema.put("type", elementSchema);
      } else {
        schema.put("items", elementSchema.get("items"));
      }

    } else {

      DeriveProtobufSchema.checkFor2dArrays(typeProtoBuf, elementSchema);
      fillRecursiveType(elementSchema, schema, calledAsField);

    }

    return schema;
  }

  private void fillRecursiveType(JSONObject element, JSONObject schema, boolean calledAsField) {

    if (calledAsField) {

      JSONObject type = new JSONObject();

      if (element.has("type") && element.get("type") instanceof JSONArray) {
        // Element is of type union
        type.put("items", element.get("type"));
      } else {
        type.put("items", element);
      }

      type.put("type", "array");
      if (typeProtoBuf) {
        type.put("__type", "array");
      }
      schema.put("type", type);

    } else {
      schema.put("items", element);
    }
  }

  public JSONObject getSchemaForRecord(JSONObject jsonObject, String name)
      throws JsonProcessingException {
    return getSchemaForRecord(jsonObject, name, false);
  }

  /**
   * Generates schema for record.
   *
   * @param message       input message for which schema is found
   * @param name          name assigned to record
   * @param calledAsField flag to check if called as a field
   * @return schema
   * @throws JsonProcessingException thrown if message not in JSON format
   */
  public JSONObject getSchemaForRecord(JSONObject message, String name,
                                       boolean calledAsField) throws JsonProcessingException {

    JSONObject schema = new JSONObject();

    if (typeProtoBuf) {
      schema.put("__type", "record");
    }

    if (!calledAsField) {
      schema.put("type", "record");
    }

    schema.put("name", name);
    schema.put("fields", new JSONArray());

    // Using TreeSet to visit fields in sorted manner
    TreeSet<String> keySet = new TreeSet<>(message.keySet());

    for (String key : keySet) {

      checkName(key);
      Object field = message.get(key);
      Optional<JSONObject> primitiveSchema = getPrimitiveSchema(field);

      if (primitiveSchema.isPresent()) {

        JSONArray fields = (JSONArray) schema.get("fields");
        JSONObject info = primitiveSchema.get();
        info.put("name", key);
        fields.put(info);

      } else {

        JSONObject info;
        if (isArrayType(field.getClass().toString())) {
          info = getSchemaForArray(getListFromArray(field), key, true, false);
        } else {
          info = getSchemaForRecord(getMapFromObject(field), key, true);
        }

        JSONArray fields = (JSONArray) schema.get("fields");
        fields.put(info);

      }
    }

    if (calledAsField) {
      JSONObject recursive = new JSONObject();
      recursive.put("fields", schema.get("fields"));
      recursive.put("type", "record");

      if (typeProtoBuf) {
        recursive.put("__type", "record");
      }

      recursive.put("name", name);
      schema.remove("fields");
      schema.put("type", recursive);
    }

    return schema;
  }

  private static void checkValidSchema(JSONObject schema) {
    new AvroSchema(schema.toString());
    if (schema.getJSONArray("fields").isEmpty()) {
      throw new IllegalArgumentException("Ignoring Empty record passed.");
    }
  }

  /**
   * Get schema for multiple messages.
   * <p>
   * For lenient check, treated same as array of records and exactly one schema is returned
   * </p>
   * <p>
   * For Strict check, the schemas are merged for number types and union types.
   * Top 3 occurring schemas are returned
   * </p>
   *
   * @param messages list of messages, each message is a string
   * @return map with schema and the number of messages it matches
   * @throws JsonProcessingException  thrown if message not in JSON format
   * @throws IllegalArgumentException thrown if no messages can be generated for strict check
   */
  public List<JSONObject> getSchemaForMultipleMessages(List<String> messages)
      throws JsonProcessingException {

    ArrayList<Object> messageObjects = new ArrayList<>();
    for (String message : messages) {
      messageObjects.add(new JSONObject(message));
    }

    /*
    Lenient check returns 1 schema Picking highest occurring datatype in case of conflicts
    */

    if (!strictCheck) {
      JSONObject schema = getSchemaForArray(messageObjects, "Record", false, true);
      JSONObject finalSchema = schema.getJSONObject("items");

      JSONObject schemaInfo = new JSONObject();
      schemaInfo.put("schema", finalSchema);
      return Collections.singletonList(schemaInfo);
    }

    /*
    Strict schema for each element is found
    Merging of Unions is performed then
    All Unique Schemas are returned and the messages it matches
    */

    ArrayList<JSONObject> schemaList = getSchemaOfAllElements(messageObjects,
        "Record", true);

    ArrayList<JSONObject> uniqueList = MergeJsonUtils.getUnique(schemaList);

    if (uniqueList.size() == 0) {
      logger.error(errorMessageNoSchemasFound);
      throw new IllegalArgumentException(errorMessageNoSchemasFound);
    }

    List<List<Integer>> schemaToMessagesInfo = MergeAvroProtoBufUtils.getUniqueWithMessageInfo(
        schemaList, uniqueList, null);

    MergeAvroProtoBufUtils.mergeNumberTypes(uniqueList, true);
    MergeUnionUtils.mergeUnion(uniqueList, true);

    ArrayList<JSONObject> uniqueList2 = MergeJsonUtils.getUnique(uniqueList);
    schemaToMessagesInfo = MergeAvroProtoBufUtils.getUniqueWithMessageInfo(
        uniqueList, uniqueList2, schemaToMessagesInfo);

    ArrayList<JSONObject> ans = new ArrayList<>();
    for (int i = 0; i < uniqueList2.size(); i++) {
      JSONObject schemaInfo = new JSONObject();
      List<Integer> messagesMatched = schemaToMessagesInfo.get(i);

      try {
        checkValidSchema(uniqueList2.get(i));
      } catch (Exception e) {
        String errorMessage = String.format("Messages %s: Unable to find schema. ", messagesMatched)
            + e.getMessage();
        logger.warn(errorMessage);
        continue;
      }

      schemaInfo.put("schema", uniqueList2.get(i));
      Collections.sort(messagesMatched);
      schemaInfo.put("messagesMatched", messagesMatched);
      schemaInfo.put("numMessagesMatched", schemaToMessagesInfo.get(i).size());
      ans.add(schemaInfo);
    }

    if (ans.size() == 0) {
      logger.error(errorMessageNoSchemasFound);
      throw new IllegalArgumentException(errorMessageNoSchemasFound);
    }

    Comparator<JSONObject> comparator
        = Comparator.comparingInt(obj -> obj.getInt("numMessagesMatched"));

    ans.sort(comparator.reversed());
    return ans;
  }


}