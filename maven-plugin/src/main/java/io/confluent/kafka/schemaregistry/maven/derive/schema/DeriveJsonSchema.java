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
import io.confluent.kafka.schemaregistry.maven.derive.schema.utils.JsonUtils;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 * Class with functionality to derive schema from messages for JSON.
 */
public class DeriveJsonSchema extends DeriveSchema {

  public DeriveJsonSchema() {
    fillMap();
  }

  final Map<String, String> classToDataType = new HashMap<>();

  private void fillMap() {

    classToDataType.put("class java.lang.String", "string");
    classToDataType.put("class java.lang.Integer", "number");
    classToDataType.put("class java.lang.Long", "number");
    classToDataType.put("class java.math.BigInteger", "number");
    classToDataType.put("class java.math.BigDecimal", "number");
    classToDataType.put("class java.lang.Boolean", "boolean");
    classToDataType.put("class org.json.JSONObject$Null", "null");

  }

  /**
   * Primitive schema of message is generated, if possible.
   *
   * @param field - message whose schema has to be found
   * @return JSONObject if type primitive else empty option
   */
  Optional<JSONObject> getPrimitiveSchema(Object field) {

    String jsonInferredType;
    if (field == null) {
      jsonInferredType = "class org.json.JSONObject$Null";
    } else {
      jsonInferredType = field.getClass().toString();
    }

    if (classToDataType.containsKey(jsonInferredType)) {
      String schemaString = String.format("{\"type\" : \"%s\"}",
          classToDataType.get(jsonInferredType));
      return Optional.of(new JSONObject(schemaString));
    }

    return Optional.empty();
  }

  /**
   * Get schema for each message present in list.
   *
   * @param messages list of messages, each message is a JSONObject
   * @param name     name used for record and arrays
   * @return List of Schemas, one schema for each message
   * @throws JsonProcessingException thrown if message not in JSON format
   */

  public ArrayList<JSONObject> getSchemaOfAllElements(List<Object> messages, String name)
      throws JsonProcessingException {

    ArrayList<JSONObject> arr = new ArrayList<>();
    for (Object message : messages) {
      Optional<JSONObject> x = getPrimitiveSchema(message);
      if (x.isPresent()) {
        arr.add(x.get());
      } else if (isArrayType(message.getClass().toString())) {
        arr.add(getSchemaForArray(getListFromArray(message), name));
      } else {
        arr.add(getSchemaForRecord(getMapFromObject(message), name));
      }
    }

    return arr;
  }


  private JSONObject concatElementsUsingOneOf(ArrayList<JSONObject> othersList) {

    List<JSONObject> elements = new ArrayList<>(othersList);
    JSONObject oneOf = new JSONObject();
    oneOf.put("oneOf", elements);
    return oneOf;

  }


  /**
   * Generates schema for array type.
   * <p>
   * If the array contains multiple records,
   * they are merged together into one record with optional fields
   * </p>
   * <p>
   * If the array contains multiple arrays,
   * they are merged together into one array with multiple data types
   * </p>
   *
   * @param messages List of messages, each message is a JSONObject
   * @param name     name assigned to array
   * @return schema
   * @throws JsonProcessingException thrown if message not in JSON format
   */

  public JSONObject getSchemaForArray(List<Object> messages, String name)
      throws JsonProcessingException {

    JSONObject schema = new JSONObject();
    schema.put("type", "array");

    ArrayList<JSONObject> schemaList = getSchemaOfAllElements(messages, name);
    ArrayList<JSONObject> uniqueSchemas = JsonUtils.getUnique(schemaList);
    ArrayList<JSONObject> recordList = new ArrayList<>();
    ArrayList<JSONObject> arrayList = new ArrayList<>();
    ArrayList<JSONObject> othersList = new ArrayList<>();

    for (JSONObject schemaElement : uniqueSchemas) {
      if (schemaElement.get("type").equals("object")) {
        recordList.add(schemaElement);
      } else if (schemaElement.get("type").equals("array")) {
        arrayList.add(schemaElement);
      } else {
        othersList.add(schemaElement);
      }
    }

    if (recordList.size() > 1) {
      JSONObject x = JsonUtils.mergeRecords(recordList);
      othersList.add(x);
    } else if (recordList.size() == 1) {
      othersList.add(recordList.get(0));
    }

    if (arrayList.size() > 1) {
      JSONObject x = JsonUtils.mergeArrays(arrayList);
      othersList.add(x);
    } else if (arrayList.size() == 1) {
      othersList.add(arrayList.get(0));
    }

    if (othersList.size() > 1) {
      schema.put("items", concatElementsUsingOneOf(othersList));
    } else if (othersList.size() > 0) {
      schema.put("items", othersList.get(0));
    } else {
      schema.put("items", new JSONObject());
    }

    return schema;

  }

  /**
   * Generates schema for record.
   *
   * @param message input message for which schema is found
   * @param name    name assigned to record
   * @return schema
   * @throws JsonProcessingException thrown if message not in JSON format
   */
  public JSONObject getSchemaForRecord(JSONObject message, String name)
      throws JsonProcessingException {

    JSONObject schema = new JSONObject();
    schema.put("type", "object");
    schema.put("properties", new JSONObject());

    for (String key : message.keySet()) {

      Object field = message.get(key);
      Optional<JSONObject> primitiveSchema = getPrimitiveSchema(field);
      JSONObject info;

      if (primitiveSchema.isPresent()) {
        info = primitiveSchema.get();
      } else {
        if (isArrayType(field.getClass().toString())) {
          info = getSchemaForArray(getListFromArray(field), key);
        } else {
          info = getSchemaForRecord(
              getMapFromObject(field), key);
        }
      }

      JSONObject fields = (JSONObject) schema.get("properties");
      fields.put(key, info);

    }

    return schema;
  }

  /**
   * Get schema for multiple messages.
   * Treated same as array of records and exactly one schema is returned
   *
   * @param messages list of messages, each message is a string
   * @return map with schema and the number of messages it matches
   * @throws JsonProcessingException thrown if message not in JSON format
   */
  public JSONObject getSchemaForMultipleMessages(List<String> messages)
      throws JsonProcessingException {

    List<Object> messageObjects = new ArrayList<>();
    for (String s : messages) {
      messageObjects.add(new JSONObject(s));
    }

    JSONObject schema = getSchemaForArray(messageObjects, "").getJSONObject("items");
    JSONObject ans = new JSONObject();
    ans.put("schema", schema);
    return ans;
  }


}
