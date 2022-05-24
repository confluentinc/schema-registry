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

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.TreeSet;

/**
 * Utility class for adjusting int, long, float and double values in avro and protobuf.
 */
public final class MergeNumberUtils {

  /**
   * Function to match fields of int, long, float and double values.
   *
   * @param schema1 schema to compare
   * @param schema2 schema to compare
   */
  public static void adjustNumberTypes(JSONObject schema1, JSONObject schema2) {

    if (checkName(schema1, schema2)) {
      return;
    }

    TreeSet<String> keys1 = new TreeSet<>(schema1.keySet());
    TreeSet<String> keys2 = new TreeSet<>(schema2.keySet());

    for (String key : keys1) {

      if (!keys2.contains(key) || (schema1.get(key).getClass() != schema2.get(key).getClass())) {
        continue;
      }

      String field1 = schema1.get(key).toString();
      String field2 = schema2.get(key).toString();

      if (field1.equals(field2)) {
        continue;
      }

      matchTypes(schema1, schema2, field1, field2, key);

      if (schema1.get(key) instanceof JSONArray && schema2.get(key) instanceof JSONArray) {
        matchJsonArray(schema1, schema2, key);
      } else if (schema1.get(key) instanceof JSONObject && schema2.get(key) instanceof JSONObject) {
        adjustNumberTypes(schema1.getJSONObject(key), schema2.getJSONObject(key));
      }

    }

  }

  private static boolean checkName(JSONObject schema1, JSONObject schema2) {

    if (schema1.similar(schema2)) {
      return true;
    }

    return schema1.has("name") && schema2.has("name")
        && !schema1.getString("name").equals(schema2.getString("name"));
  }

  private static void matchTypes(JSONObject schema1, JSONObject schema2,
                                 String field1, String field2, String key) {


    if (key.equals("name")) {
      return;
    }

    ArrayList<String> dataTypes = new ArrayList<>(Arrays.asList("int", "int32", "int64", "long"));

    if (Objects.equals(field2, "double") && dataTypes.contains(field1)) {
      schema1.put(key, schema2.get(key));
    } else if (Objects.equals(field1, "double") && dataTypes.contains(field2)) {
      schema2.put(key, schema1.get(key));
    } else if (Objects.equals(field2, "long") && dataTypes.contains(field1)) {
      schema1.put(key, schema2.get(key));
    } else if (Objects.equals(field1, "long") && dataTypes.contains(field2)) {
      schema2.put(key, schema1.get(key));
    }

  }

  private static void matchJsonArray(JSONObject schema1, JSONObject schema2, String key) {

    JSONArray fields1 = schema1.getJSONArray(key);
    JSONArray fields2 = schema2.getJSONArray(key);

    for (int i = 0; i < fields1.length(); i++) {
      for (int j = 0; j < fields2.length(); j++) {

        if ((fields1.get(i) instanceof JSONObject) && (fields2.get(j) instanceof JSONObject)) {

          JSONObject field1 = fields1.getJSONObject(i);
          JSONObject field2 = fields2.getJSONObject(j);

          if (field1.getString("name").equals(field2.getString("name"))) {
            adjustNumberTypes(fields1.getJSONObject(i), fields2.getJSONObject(j));
          }

        }
      }
    }

  }


}
