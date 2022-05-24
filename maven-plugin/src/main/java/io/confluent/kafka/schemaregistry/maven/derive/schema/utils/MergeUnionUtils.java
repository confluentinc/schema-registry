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
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utility class to provide functionality for unions in avro.
 */

public final class MergeUnionUtils {

  private static final Logger logger = LoggerFactory.getLogger(MergeUnionUtils.class);

  static final ArrayList<String> avroTypes =
      new ArrayList<>(Arrays.asList("float", "double", "int", "long", "string", "boolean"));


  private static void checkUnionInArray(JSONObject schema, ArrayList<String> unionBranches) {

    if (schema.get("type").equals("array") && schema.get("items") instanceof JSONObject) {
      JSONObject type = schema.getJSONObject("items");
      if (type.get("type") instanceof String) {
        String t = type.getString("type");
        if (!t.equals("record") && !t.equals("array")) {
          unionBranches.add(t);
        }
      }
    }

  }

  private static boolean checkBranchName(JSONObject fieldItem) {

    String name = fieldItem.getString("name");
    if (!(fieldItem.get("type") instanceof String)) {
      return false;
    }
    String type = fieldItem.getString("type");

    if (avroTypes.contains(name)) {

      List<String> numberTypes = Arrays.asList("int", "double", "float", "long");
      if (numberTypes.contains(name)) {
        return numberTypes.contains(type);
      } else {
        return type.equals(name);
      }

    }

    return false;
  }

  static void checkUnionInRecord(JSONObject obj,
                                 ArrayList<String> unionBranches,
                                 boolean includeComplex) {

    JSONArray fields = obj.getJSONArray("fields");
    if (fields.length() == 1 && fields.get(0) instanceof JSONObject) {

      JSONObject fieldItem = fields.getJSONObject(0);
      if (checkBranchName(fieldItem)) {
        // Primitive Type
        unionBranches.add(fieldItem.getString("name"));
      } else if (includeComplex) {

        // Complex Type - Array or Record or Union
        Object type = fieldItem.get("type");
        if (type.equals("record") || type.equals("array") || !(type instanceof String)) {
          unionBranches.add(fieldItem.toString());
        }

      }

    }

  }


  /**
   * Returns branch of union if present otherwise empty.
   * For primitive data types name is chosen as datatype and matching with inferred datatype
   * is not done currently.
   * <p><strong>Sample Input </strong></p>
   * <p>schema1
   * -
   * <pre>{@code
   * {
   *   "name":"log_id",
   *   "type":{
   *     "name":"log_id",
   *     "fields":[
   *       {
   *         "name":"long",
   *         "type":"int"
   *       }
   *     ],
   *     "type":"record"
   *   }
   * }
   * }</pre>
   * includeComplex true
   * </p>
   * <p><strong> Output </strong></p>
   * <pre>{@code ['long']}</pre>
   *
   * @param schema         Object to check for union
   * @param includeComplex flag to include records and arrays in list
   * @return branch of union as a list
   **/

  static ArrayList<String> checkForUnion(JSONObject schema, boolean includeComplex) {

    ArrayList<String> unionBranches = new ArrayList<>();
    JSONObject obj = schema;

    if (schema.get("type") instanceof JSONObject) {
      obj = schema.getJSONObject("type");
    }

    if ((obj.has("fields"))) {
      checkUnionInRecord(obj, unionBranches, includeComplex);
    }

    checkUnionInArray(schema, unionBranches);
    return unionBranches;
  }

  /**
   * Convert string to JSONObject if possible and make changes in type, fields and items.
   *
   * @param str string to convert to JSONObject
   * @param arr array to which JSONObject is added
   */

  private static boolean convertStringToObject(String str, List<Object> arr) {

    try {

      JSONObject jsonObject = new JSONObject(str);

      if (jsonObject.get("type") instanceof JSONObject) {
        if (jsonObject.getJSONObject("type").has("fields")) {
          jsonObject.put("fields", jsonObject.getJSONObject("type").getJSONArray("fields"));
          jsonObject.put("type", "record");
        } else if (jsonObject.getJSONObject("type").has("items")) {
          if (!jsonObject.get("name").equals("array")) {
            logger.warn(String.format("For type Union, branch must have name array "
                + "instead of %s", jsonObject.get("name")));
            return false;
          }
          jsonObject.put("items", jsonObject.getJSONObject("type").get("items"));
          jsonObject.put("type", "array");
        }
      }

      arr.add(jsonObject);

    } catch (JSONException e) {
      arr.add(str);
    }

    return true;
  }

  private static void copyTypesForArray(JSONObject schema1, JSONObject schema2,
                                        Set<String> unionBranches) {

    if (schema2.has("items")) {

      if (schema2.get("items") instanceof JSONArray) {
        JSONArray arr = schema2.getJSONArray("items");
        if (arr.length() == 0) {
          return;
        }

        boolean flag = true;
        for (Object x : arr) {
          if (!unionBranches.contains(x.toString())) {
            flag = false;
            break;
          }
        }

        if (flag) {
          schema2.put("items", schema1.get("items"));
        }

      } else if ((schema2.get("items") instanceof String)
          && unionBranches.contains(schema2.getString("items"))) {
        schema2.put("items", schema1.get("items"));
      }

    }
  }

  /**
   * Check if branch of union is schema2 is present in schema1, if yes copy update schema2.
   *
   * @param schema1 Object with all branches of union
   * @param schema2 Object to which changes are copied
   */
  static void copyTypes(JSONObject schema1, JSONObject schema2) {

    Set<String> unionBranches = new HashSet<>();
    fillBranches(schema1, unionBranches, new HashSet<>(), false);

    if (schema2.get("type").equals("null")) {
      if (unionBranches.contains("null")) {
        schema2.put("type", schema1.get("type"));
        return;
      }
    }

    ArrayList<String> newUnionBranches = checkForUnion(schema2, true);
    if (newUnionBranches.size() > 0 && unionBranches.contains(newUnionBranches.get(0))) {
      if (!(schema1.get("type") instanceof String)) {
        schema2.put("type", schema1.get("type"));
        schema2.remove("fields");
      } else {
        schema2.put("type", schema1.get("type"));
        schema2.put("fields", schema1.get("fields"));
      }
    }

    // Case when schema2 is type union
    if (schema2.get("type") instanceof JSONArray) {
      JSONArray arr = schema2.getJSONArray("type");
      if (arr.length() == 0) {
        return;
      }
      Object schema2Type = schema2.getJSONArray("type").get(0);
      if (unionBranches.contains(schema2Type.toString())) {
        schema2.put("type", schema1.get("type"));
      }
    }

    copyTypesForArray(schema1, schema2, unionBranches);

  }

  /**
   * Helper function to add given string to union and names set.
   *
   * @param str            string to add
   * @param unionBranches  set of all branches
   * @param namesOfObjects set of names of records in union
   * @param check          to check if name present in unionBranches or not
   */
  private static void addToUnion(String str, Set<String> unionBranches,
                                 Set<String> namesOfObjects, boolean check) {

    try {
      JSONObject temp = new JSONObject(str);
      if (check) {
        String name = temp.getString("name");
        if (!namesOfObjects.contains(name)) {
          unionBranches.add(str);
        }
      } else {
        unionBranches.add(str);
        namesOfObjects.add(temp.getString("name"));
      }
    } catch (JSONException e) {
      unionBranches.add(str);
    }

  }


  private static void fillBranches(JSONObject schema, Set<String> unionBranches,
                                   Set<String> namesOfObjects, boolean check) {


    if (schema.get("type").equals("null")) {
      unionBranches.add("null");
    } else if (schema.get("type") instanceof JSONArray) {

      // schema1 is of type Union, going through all branches
      for (Object obj : schema.getJSONArray("type")) {
        addToUnion(obj.toString(), unionBranches, namesOfObjects, check);
      }

    } else if (schema.has("items") && schema.get("items") instanceof JSONArray) {

      for (Object obj : schema.getJSONArray("items")) {
        addToUnion(obj.toString(), unionBranches, namesOfObjects, check);
      }

    } else {

      ArrayList<String> newUnionBranches = checkForUnion(schema, true);
      if (newUnionBranches.size() == 1) {
        addToUnion(newUnionBranches.get(0), unionBranches, namesOfObjects, check);
      }

    }

  }

  /**
   * Alter schema1 to have branches of union in schema1
   * and schema2.
   *
   * @param schema1 JSONObject where changes are made
   * @param schema2 JSONObject where no change is made
   */
  static void matchTypes(JSONObject schema1, JSONObject schema2) {

    /*
    Branches of the union are found out and merged here
    */

    Set<String> unionBranches = new HashSet<>();
    Set<String> namesOfObjects = new HashSet<>();

    fillBranches(schema1, unionBranches, namesOfObjects, false);
    fillBranches(schema2, unionBranches, namesOfObjects, true);

    if (unionBranches.size() > 1) {

      List<Object> arr = new ArrayList<>();
      boolean flag = true;
      for (String x : unionBranches) {
        flag = flag && convertStringToObject(x, arr);
      }

      arr.sort(Comparator.comparing(Object::toString));

      if (flag) {

        if (schema1.get("type").equals("array")) {
          schema1.put("items", arr);
        } else {
          schema1.put("type", arr);
        }

        if (schema1.has("fields")) {
          schema1.remove("fields");
        }

      }

    }

  }

  /**
   * Match schema1
   * with type of schema2.
   *
   * @param schema1      JSONObject matched whole
   * @param schema2      JSONObject whose type is matched
   * @param changeSecond flag to change schema2
   */
  static void matchRecordTypeWithRecord(JSONObject schema1, JSONObject schema2,
                                        boolean changeSecond) {

    /*
    Eg, schema1 {name: length, type: [obj, null]}
        schema2 {name: length, type: {name: length, fields: [obj]} }
        Here, obj itself could have unions and hence, we need to match schema1 and schema2's type
        and go deeper
        Avro's syntax is such that type record is not mentioned, instead type is recursively defined
     */


    if ((schema2.get("type") instanceof JSONObject) && !schema1.similar(schema2)) {

      JSONObject schema2Type = schema2.getJSONObject("type");
      if (schema2Type.has("name") && schema1.get("name").equals(schema2Type.get("name"))) {
        matchJsonArray(schema1, schema2.getJSONObject("type"), changeSecond);
      }

      if (schema2Type.similar(schema1)) {
        schema2.put("type", "record");
        schema2.put("fields", schema1.get("fields"));
      }

    }


  }

  /**
   * Each branch in union of schema1
   * is matched with field/type in schema2.
   *
   * @param schema1      JSONObject of type union
   * @param schema2      JSONObject of type record
   * @param changeSecond flag to change schema2
   */
  static void matchJsonArrayWithRecord(JSONObject schema1,
                                       JSONObject schema2, boolean changeSecond) {

    JSONArray arr = schema1.getJSONArray("type");
    ArrayList<JSONObject> arrJsonObjects = new ArrayList<>();

    for (int i = 0; i < arr.length(); i++) {
      if (arr.get(i) instanceof JSONObject) {
        arrJsonObjects.add(arr.getJSONObject(i));
      }
    }

    /*
    Matching Object of same name in schema1 to Object in schema2
    Eg, schema1 {name: J, type: [Obj1, Obj2, null]}
        schema2 {name: J, type: {J, fields:[Obj1]}}
        To match Obj1 in both schema1 and schema2
    */
    if (schema2.get("type") instanceof JSONObject) {

      JSONObject toCompare = schema2.getJSONObject("type");
      if (schema1.get("name").equals(toCompare.get("name"))) {

        for (JSONObject obj : arrJsonObjects) {
          JSONObject temp = new JSONObject();
          temp.put("name", schema1.getString("name"));

          JSONArray f = new JSONArray();
          f.put(obj);
          temp.put("fields", f);
          temp.put("type", "record");

          matchJsonArray(temp, toCompare, changeSecond);
        }
      }

    } else if (schema2.has("fields")) {

      for (JSONObject obj : arrJsonObjects) {
        if (obj.get("name").equals(schema2.getJSONArray("fields").getJSONObject(0).get("name"))) {
          matchJsonArray(obj, schema2.getJSONArray("fields").getJSONObject(0), changeSecond);
        }
      }

    }

  }

  /**
   * Match objects inside fields of schema1
   * and schema2.
   *
   * @param schema1      JSONObject of type record
   * @param schema2      JSONObject of type record
   * @param changeSecond flag to change schema2
   */

  private static void matchFieldsInArray(JSONObject schema1,
                                         JSONObject schema2, boolean changeSecond) {

    if (schema1.has("fields") && schema1.get("fields") instanceof JSONArray
        && schema2.get("fields") instanceof JSONArray) {

      JSONArray schema1Fields = schema1.getJSONArray("fields");
      JSONArray schema2Fields = schema2.getJSONArray("fields");

      for (int i = 0; (i < schema1Fields.length()) && (i < schema2Fields.length()); ++i) {
        if (schema1Fields.getJSONObject(i).get("name")
            .equals(schema2Fields.getJSONObject(i).get("name"))) {
          matchJsonArray(schema1Fields.getJSONObject(i),
              schema2Fields.getJSONObject(i), changeSecond);
        }
      }
    }
  }

  static void matchItems(JSONObject schema1,
                         JSONObject schema2, boolean changeSecond) {


    /*
    Matching Object of same name in schema1 to Object in schema2
    Eg, schema1 {name: J, items: [Obj1, Obj2, null]}
        schema2 {name: J, items: [Obj1, Obj3, null]}
        To match Obj1 in both schema1 and schema2
        Possible only for array of unions
    */

    if ((schema1.get("items") instanceof JSONArray)
        && (schema2.get("items") instanceof JSONArray)) {

      JSONArray schema1Items = schema1.getJSONArray("items");
      JSONArray schema2Items = schema2.getJSONArray("items");

      for (int i = 0; i < schema1Items.length(); ++i) {

        if (schema1Items.get(i) instanceof JSONObject) {
          for (int j = 0; j < schema2Items.length(); j++) {
            if (schema2Items.get(j) instanceof JSONObject) {
              if (schema1Items.getJSONObject(i).getString("name")
                  .equals(schema2Items.getJSONObject(j).getString("name"))) {
                matchJsonArray(schema1Items.getJSONObject(i),
                    schema2Items.getJSONObject(j), changeSecond);
              }
            }
          }
        }

      }

    }

  }

  /**
   * Match type or field of schema1
   * with corresponding type or field with schema2.
   *
   * @param schema1      JSONObject to match
   * @param schema2      JSONObject to match
   * @param changeSecond flag to change schema2
   */

  private static void matchJsonArray(JSONObject schema1, JSONObject schema2, boolean changeSecond) {

    if (schema1.similar(schema2)) {
      return;
    }

    if (schema1.get("type") instanceof JSONObject && schema2.get("type") instanceof JSONObject) {
      /*
      Both are of type record as type is JSONObject
      */
      matchJsonArray(schema1.getJSONObject("type"), schema2.getJSONObject("type"), changeSecond);
    } else if (schema1.get("type") instanceof JSONArray) {

      /*
      schema1 has type Union, type is of json Array,
      Eg, type:{long, double, object}
      */

      matchJsonArrayWithRecord(schema1, schema2, changeSecond);
    } else if (schema1.has("fields") && schema2.has("fields")) {

      /*
      Both schema1 and schema2, type records and matching fields
      */

      matchFieldsInArray(schema1, schema2, changeSecond);
    } else if (schema1.has("items") && schema2.has("items")) {
      /*
      Both schema1 and schema2, both of type array of unions
      */
      matchItems(schema1, schema2, changeSecond);
    } else if (!schema2.has("fields")) {
      /*
      Both schema1 and schema2, type records and matching type of one with other
      */
      matchRecordTypeWithRecord(schema1, schema2, changeSecond);
    }

    // Update schema1 and schema2 according to loop 1 or 2
    updateRecords(schema1, schema2, changeSecond);

  }

  private static void updateRecords(JSONObject schema1, JSONObject schema2, boolean changeSecond) {

    if (!schema1.similar(schema2)) {

      if (!changeSecond) {
        // Merging types, schema1 is changed in this function to have all branches of union
        matchTypes(schema1, schema2);

      } else {
        // schema2 is copied from schema1, if the branch of schema2 is present in union
        copyTypes(schema1, schema2);
      }

      if (changeSecond && (schema2.get("type") instanceof JSONObject)) {
        updateMergedRecords(schema1, schema2);
        if (!schema2.get("type").equals("record")) {
          updateMergedArrays(schema1, schema2);
        }
      }

    }

  }

  /**
   * Update record schema2 to have fields and type as schema1
   * .
   *
   * @param schema1 JSONObject with correct formatting
   * @param schema2 JSONObject to update
   */
  private static void updateMergedRecords(JSONObject schema1, JSONObject schema2) {

    /*
    Inside unions type:record has to be mentioned and fields
    Eg, schema1 {name:J, type: record, fields:[obj1, obj2]}
        schema2 {name, J, type: {name: J, fields[obj1, obj2]}
        Here schema2's type is replaced with record and fields are same as schema1
        's
     */

    JSONObject schema2Type = schema2.getJSONObject("type");
    if (schema1.similar(schema2Type) && schema1.has("fields")) {
      schema2.put("type", schema1.get("type"));
      schema2.put("fields", schema1.get("fields"));
    }

  }

  /**
   * Update array schema2 to have items and type as schema1
   * .
   *
   * @param schema1 JSONObject with correct formatting
   * @param schema2 JSONObject to update
   */

  private static void updateMergedArrays(JSONObject schema1, JSONObject schema2) {

    /*
    Restructuring Array items, similar to record
    Eg, schema1 {"name":"array","type":"array","items":"int"}
        schema2 {"name":"array","type":{"type":"array","items":"int"}}
        schema2 is changed to same as schema1
     */
    JSONObject schema2Type = schema2.getJSONObject("type");

    if (schema1.get("type").equals("array") && schema2Type.has("items")) {

      if ((schema2Type.get("items") instanceof String)
          && schema2Type.get("items").equals(schema1.get("items"))) {

        // 1D Array
        schema2.put("type", "array");
        schema2.put("items", schema1.get("items"));

      } else if ((schema2Type.get("items") instanceof JSONObject)
          && schema2Type.getJSONObject("items").similar(schema1.get("items"))) {

        // 2D or higher Array
        schema2.put("type", "array");
        schema2.put("items", schema1.get("items"));

      }

    }

  }

  /**
   * Function to match and update schemas to include unions.
   *
   * @param schemaList List of schemas
   */
  public static void mergeUnion(ArrayList<JSONObject> schemaList, boolean matchAll) {

    if (schemaList.size() == 0) {
      return;
    }

    for (JSONObject curr : schemaList) {
      compressRecordToUnion(curr);
    }

    if (matchAll) {
      for (JSONObject obj : schemaList) {
        mergeUnionLoops(schemaList, obj);
      }
    } else {
      mergeUnionLoops(schemaList, schemaList.get(0));
    }

  }


  private static void mergeUnionLoops(ArrayList<JSONObject> schemaList, JSONObject starting) {

    // Loop 1 : Match Schema 1 with rest and modify only first schema to include all union branches
    for (JSONObject curr : schemaList) {
      if (!starting.similar(curr)) {
        matchJsonArray(starting, curr, false);
      }
    }

    // Loop 2 : Match Schema 1 with rest and check if second has corresponding branch in union
    // If yes, then copy union from schema 1 to current schema
    for (JSONObject curr : schemaList) {
      if (!starting.similar(curr)) {
        matchJsonArray(starting, curr, true);
      }
    }

  }

  /**
   * Compress record to type union, if possible.
   * <pre>{@code
   * {
   *   "name": "server_script_path",
   *   "type": {
   *     "name": "server_script_path",
   *     "fields": [{
   *       "name": "string",
   *       "type": "string"
   *     }],
   *     "type": "record"
   *   }
   * }}</pre>
   * is changed to
   * <pre>{@code
   * {
   * "name":"server_script_path",
   * "type":["string"]
   * }}</pre>
   *
   * @param schema JSONObject to compress
   */


  static void compressRecordToUnion(JSONObject schema) {

    ArrayList<String> unionBranches = checkForUnion(schema, false);
    if (unionBranches.size() == 1) {
      JSONArray union = new JSONArray();
      union.put(unionBranches.get(0));
      if (schema.get("type").equals("array")) {
        schema.put("items", unionBranches.get(0));
      } else {
        schema.put("type", union);
        schema.remove("fields");
      }
      return;
    }

    for (String typeName : Arrays.asList("type", "fields")) {
      if (schema.has(typeName) && schema.get(typeName) instanceof JSONArray) {
        JSONArray arr = schema.getJSONArray(typeName);
        for (int i = 0; i < arr.length(); i++) {
          if (arr.get(i) instanceof JSONObject) {
            compressRecordToUnion(arr.getJSONObject(i));
          }
        }
      }
    }

    if (schema.has("type") && schema.get("type") instanceof JSONObject) {
      compressRecordToUnion(schema.getJSONObject("type"));
    }

  }

}
