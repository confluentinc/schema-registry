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
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;


/**
 * Abstract class to provide structure for each schema type and store common functions.
 */
public abstract class DeriveSchema {


  abstract Optional<JSONObject> getPrimitiveSchema(Object field);

  abstract ArrayList<JSONObject> getSchemaOfAllElements(List<Object> list, String name)
      throws JsonProcessingException;

  abstract JSONObject getSchemaForArray(List<Object> list, String name)
      throws JsonProcessingException;

  abstract JSONObject getSchemaForRecord(JSONObject jsonObject, String name)
      throws JsonProcessingException;

  static boolean isArrayType(String javaClass) {
    return javaClass.equals("class org.json.JSONArray")
        || javaClass.equals("class java.util.ArrayList");
  }

  static List<Object> getListFromArray(Object field) {

    if (field instanceof JSONArray) {
      return ((JSONArray) field).toList();
    }

    return ((List<Object>) field);
  }

  static JSONObject getMapFromObject(Object field) {

    if (field instanceof JSONObject) {
      return (JSONObject) field;
    }

    return new JSONObject((HashMap) field);
  }

}
