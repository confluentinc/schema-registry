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

package io.confluent.kafka.schemaregistry.maven.derive.schema.avro;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema.mapper;

public final class MergeUnionUtils {

  private static final Logger logger = LoggerFactory.getLogger(MergeUnionUtils.class);

  static final ArrayList<String> avroTypes =
      new ArrayList<>(Arrays.asList("float", "double", "int", "long", "string", "boolean"));


  private static String nodeToString(JsonNode node) {

    if (node instanceof TextNode) {
      return node.asText();
    }

    return node.toString();
  }

  private static void checkUnionInArray(ObjectNode schema, ArrayList<String> unionBranches) {

    if (schema.get("type").asText().equals("array")
        && schema.get("items") instanceof ObjectNode) {
      ObjectNode type = (ObjectNode) schema.get("items");
      if (type.get("type") instanceof TextNode) {
        String t = type.get("type").asText();
        if (!t.equals("record") && !t.equals("array")) {
          unionBranches.add(t);
        }
      }
    }

  }

  private static boolean checkBranchName(ObjectNode fieldItem) {

    String name = fieldItem.get("name").asText();
    if (!(fieldItem.get("type") instanceof TextNode)) {
      return false;
    }
    String type = fieldItem.get("type").asText();

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

  static void checkUnionInRecord(ObjectNode obj,
                                 ArrayList<String> unionBranches,
                                 boolean includeComplex) {

    ArrayNode fields = (ArrayNode) obj.get("fields");
    if (fields.size() == 1 && fields.get(0) instanceof ObjectNode) {

      // For primitive data types name is chosen as datatype and matched with inferred datatype
      ObjectNode fieldItem = (ObjectNode) fields.get(0);
      if (checkBranchName(fieldItem)) {
        // Primitive Type
        unionBranches.add(fieldItem.get("name").asText());
      } else if (includeComplex) {

        // Complex Type - Array or Record or Union
        Object type = fieldItem.get("type");
        if (type instanceof TextNode) {
          String typeName = ((TextNode) type).asText();
          if (typeName.equals("record") || typeName.equals("array")) {
            unionBranches.add(fieldItem.toString());
          }
        } else {
          unionBranches.add(fieldItem.toString());
        }

      }

    }

  }


  static ArrayList<String> checkForUnion(ObjectNode schema, boolean includeComplex) {

    /*
    Returns branch of union if present otherwise empty.

    Eg, schema1 {name: length, type: {name: length, fields:{name:long, type:long}}
    Field can be interpreted of type unions with branch long
    Output for the function is [long]
    */

    ArrayList<String> unionBranches = new ArrayList<>();
    ObjectNode obj = schema;

    if (schema.get("type") instanceof ObjectNode) {
      obj = (ObjectNode) schema.get("type");
    }

    if ((obj.has("fields"))) {
      checkUnionInRecord(obj, unionBranches, includeComplex);
    }

    checkUnionInArray(schema, unionBranches);
    return unionBranches;
  }


  private static boolean convertStringToObject(String str, List<Object> arr) {

    // Convert string to ObjectNode if possible and make changes in type, fields and items.
    try {

      ObjectNode objectNode = mapper.readValue(str, ObjectNode.class);
      if (objectNode == null) {
        arr.add(str);
        return true;
      }

      if (objectNode.get("type") instanceof ObjectNode) {
        if (objectNode.get("type").has("fields")) {
          objectNode.set("fields", objectNode.get("type").get("fields"));
          objectNode.put("type", "record");
        } else if (objectNode.get("type").has("items")) {
          if (!objectNode.get("name").asText().equals("array")) {
            logger.warn(String.format("Message %d: For type Union, branch must have name array "
                + "instead of %s", DeriveAvroSchema.getCurrentMessage(), objectNode.get("name")));
            return false;
          }
          objectNode.set("items", objectNode.get("type").get("items"));
          objectNode.put("type", "array");
        }
      }

      arr.add(objectNode);

    } catch (JsonProcessingException e) {
      arr.add(str);
    }

    return true;
  }

  private static void copyTypesForArray(ObjectNode schema1, ObjectNode schema2,
                                        Set<String> unionBranches) {

    if (schema2.has("items")) {

      if (schema2.get("items") instanceof ArrayNode) {
        ArrayNode items = (ArrayNode) schema2.get("items");
        if (items.size() == 0) {
          return;
        }

        boolean flag = true;
        for (JsonNode branches : items) {
          String branchString = nodeToString(branches);
          if (!unionBranches.contains(branchString)) {
            flag = false;
            break;
          }
        }

        if (flag) {
          schema2.set("items", schema1.get("items"));
        }

      } else if ((schema2.get("items") instanceof TextNode)
          && unionBranches.contains(schema2.get("items").asText())) {
        schema2.set("items", schema1.get("items"));
      }

    }
  }


  static void copyTypes(ObjectNode schema1, ObjectNode schema2) {

    /*
    Checking if branch of union is schema2 is present in schema1,
    if yes copy update schema2.
    */

    Set<String> unionBranches = new HashSet<>();
    fillBranches(schema1, unionBranches, new HashSet<>(), false);

    if (schema2.get("type").asText().equals("null")) {
      if (unionBranches.contains("null")) {
        schema2.set("type", schema1.get("type"));
        return;
      }
    }

    ArrayList<String> newUnionBranches = checkForUnion(schema2, true);
    if (newUnionBranches.size() > 0 && unionBranches.contains(newUnionBranches.get(0))) {
      if (!(schema1.get("type") instanceof TextNode)) {
        schema2.set("type", schema1.get("type"));
        schema2.remove("fields");
      } else {
        schema2.set("type", schema1.get("type"));
        schema2.set("fields", schema1.get("fields"));
      }
    }

    // Case when schema2 is type union
    copyTypesForUnion(schema1, schema2, unionBranches);

    copyTypesForArray(schema1, schema2, unionBranches);

  }


  private static void copyTypesForUnion(ObjectNode schema1, ObjectNode schema2,
                                        Set<String> unionBranches) {

    if (schema2.get("type") instanceof ArrayNode) {
      ArrayNode arr = (ArrayNode) schema2.get("type");
      if (arr.size() == 0) {
        return;
      }

      JsonNode schema2Type = schema2.get("type").get(0);
      String schema2TypeString = nodeToString(schema2Type);

      if (unionBranches.contains(schema2TypeString)) {
        schema2.set("type", schema1.get("type"));
      }
    }

  }

  private static void addToUnion(String str, Set<String> unionBranches,
                                 Set<String> namesOfObjects, boolean check) {

    try {
      ObjectNode branch = mapper.readValue(str, ObjectNode.class);
      if (branch == null) {
        unionBranches.add(str);
        return;
      }

      if (check) {
        String name = branch.get("name").asText();
        if (!namesOfObjects.contains(name)) {
          unionBranches.add(str);
        }
      } else {
        unionBranches.add(str);
        namesOfObjects.add(branch.get("name").asText());
      }
    } catch (JsonProcessingException e) {
      unionBranches.add(str);
    }

  }


  private static void fillBranches(ObjectNode schema, Set<String> unionBranches,
                                   Set<String> namesOfObjects, boolean check) {


    if (schema.get("type").asText().equals("null")) {
      unionBranches.add("null");
    } else if (schema.get("type") instanceof ArrayNode) {

      // schema1 is of type Union, going through all branches
      for (JsonNode obj : schema.get("type")) {
        String objString = nodeToString(obj);
        addToUnion(objString, unionBranches, namesOfObjects, check);
      }

    } else if (schema.has("items") && schema.get("items") instanceof ArrayNode) {

      for (JsonNode obj : schema.get("items")) {
        String objString = nodeToString(obj);
        addToUnion(objString, unionBranches, namesOfObjects, check);
      }

    } else {

      ArrayList<String> newUnionBranches = checkForUnion(schema, true);
      if (newUnionBranches.size() == 1) {
        addToUnion(newUnionBranches.get(0), unionBranches, namesOfObjects, check);
      }

    }

  }

  static void matchTypes(ObjectNode schema1, ObjectNode schema2) {

    /*
    Branches of the union are found out and merged here
    Alter schema1 to have branches of union in schema1 and schema2
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

        ArrayNode arrayNode;
        if (schema1.get("type").asText().equals("array")) {
          arrayNode = schema1.putArray("items");
        } else {
          arrayNode = schema1.putArray("type");
        }

        for (Object obj : arr) {
          if (obj instanceof String) {
            arrayNode.add((String) obj);
          } else {
            arrayNode.add((JsonNode) obj);
          }
        }

        if (schema1.has("fields")) {
          schema1.remove("fields");
        }

      }

    }

  }

  static void matchRecordTypeWithRecord(ObjectNode schema1, ObjectNode schema2,
                                        boolean changeSecond) {

    /*
    Eg, schema1 {name: length, type: [obj, null]}
        schema2 {name: length, type: {name: length, fields: [obj]} }
        Here, obj itself could have unions and hence, we need to match schema1 and schema2's type
        and go deeper
        Avro's syntax is such that type record is not mentioned, instead type is recursively defined
     */


    if ((schema2.get("type") instanceof ObjectNode) && !schema1.equals(schema2)) {

      ObjectNode schema2Type = (ObjectNode) schema2.get("type");
      if (schema2Type.has("name") && schema1.get("name").equals(schema2Type.get("name"))) {
        matchArrayNode(schema1, (ObjectNode) schema2.get("type"), changeSecond);
      }

      if (schema2Type.equals(schema1)) {
        schema2.put("type", "record");
        schema2.set("fields", schema1.get("fields"));
      }

    }


  }


  static void matchJsonArrayWithRecord(ObjectNode schema1,
                                       ObjectNode schema2, boolean changeSecond) {

    ArrayNode arr = (ArrayNode) schema1.get("type");
    ArrayList<ObjectNode> arrObjects = new ArrayList<>();

    for (int i = 0; i < arr.size(); i++) {
      if (arr.get(i) instanceof ObjectNode) {
        arrObjects.add((ObjectNode) arr.get(i));
      }
    }

    /*
    Matching Object of same name in schema1 to Object in schema2
    Eg, schema1 {name: J, type: [Obj1, Obj2, null]}
        schema2 {name: J, type: {J, fields:[Obj1]}}
        To match Obj1 in both schema1 and schema2
    */
    if (schema2.get("type") instanceof ObjectNode) {

      ObjectNode toCompare = (ObjectNode) schema2.get("type");
      if (schema1.get("name").equals(toCompare.get("name"))) {

        for (ObjectNode obj : arrObjects) {
          ObjectNode temp = mapper.createObjectNode();
          temp.put("name", schema1.get("name").asText());
          temp.put("type", "record");

          ArrayNode fields = mapper.createArrayNode();
          fields.add(obj);
          temp.set("fields", fields);

          matchArrayNode(temp, toCompare, changeSecond);
        }
      }

    } else if (schema2.has("fields")) {

      for (ObjectNode obj : arrObjects) {
        if (obj.get("name").equals(schema2.get("fields").get(0).get("name"))) {
          matchArrayNode(obj, (ObjectNode) schema2.get("fields").get(0), changeSecond);
        }
      }

    }

  }

  private static void matchFieldsInArray(ObjectNode schema1,
                                         ObjectNode schema2, boolean changeSecond) {

    // Match objects inside fields of schema1 and schema2

    if (schema1.has("fields") && schema1.get("fields") instanceof ArrayNode
        && schema2.get("fields") instanceof ArrayNode) {

      ArrayNode schema1Fields = (ArrayNode) schema1.get("fields");
      ArrayNode schema2Fields = (ArrayNode) schema2.get("fields");

      for (int i = 0; (i < schema1Fields.size()) && (i < schema2Fields.size()); ++i) {
        if (schema1Fields.get(i).get("name")
            .equals(schema2Fields.get(i).get("name"))) {
          matchArrayNode((ObjectNode) schema1Fields.get(i),
              (ObjectNode) schema2Fields.get(i), changeSecond);
        }
      }
    }
  }

  static void matchItems(ObjectNode schema1,
                         ObjectNode schema2, boolean changeSecond) {


    /*
    Matching Object of same name in schema1 to Object in schema2
    Eg, schema1 {name: J, items: [Obj1, Obj2, null]}
        schema2 {name: J, items: [Obj1, Obj3, null]}
        To match Obj1 in both schema1 and schema2
        Possible only for array of unions
    */

    if ((schema1.get("items") instanceof ArrayNode)
        && (schema2.get("items") instanceof ArrayNode)) {

      ArrayNode schema1Items = (ArrayNode) schema1.get("items");
      ArrayNode schema2Items = (ArrayNode) schema2.get("items");

      for (int i = 0; i < schema1Items.size(); ++i) {

        if (schema1Items.get(i) instanceof ObjectNode) {
          for (int j = 0; j < schema2Items.size(); j++) {
            if (schema2Items.get(j) instanceof ObjectNode) {
              if (schema1Items.get(i).get("name").asText()
                  .equals(schema2Items.get(j).get("name").asText())) {
                matchArrayNode((ObjectNode) schema1Items.get(i),
                    (ObjectNode) schema2Items.get(j), changeSecond);
              }
            }
          }
        }

      }

    }

  }

  private static void matchArrayNode(ObjectNode schema1, ObjectNode schema2, boolean changeSecond) {

    if (schema1.equals(schema2)) {
      return;
    }

    if (schema1.get("type") instanceof ObjectNode && schema2.get("type") instanceof ObjectNode) {
      /*
      Both are of type record as type is ObjectNode
      */
      matchArrayNode((ObjectNode) schema1.get("type"),
          (ObjectNode) schema2.get("type"), changeSecond);
    } else if (schema1.get("type") instanceof ArrayNode) {

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

  private static void updateRecords(ObjectNode schema1, ObjectNode schema2, boolean changeSecond) {

    if (!schema1.equals(schema2)) {

      if (!changeSecond) {
        // Merging types, schema1 is changed in this function to have all branches of union
        matchTypes(schema1, schema2);

      } else {
        // schema2 is copied from schema1, if the branch of schema2 is present in union
        copyTypes(schema1, schema2);
      }

      if (changeSecond && (schema2.get("type") instanceof ObjectNode)) {
        updateMergedRecords(schema1, schema2);
        if (!schema2.get("type").asText().equals("record")) {
          updateMergedArrays(schema1, schema2);
        }
      }

    }

  }

  private static void updateMergedRecords(ObjectNode schema1, ObjectNode schema2) {

    /*

    Update record schema2 to have fields and type as schema1
    Inside unions type:record has to be mentioned and fields
    Eg, schema1 {name:J, type: record, fields:[obj1, obj2]}
        schema2 {name, J, type: {name: J, fields[obj1, obj2]}
        Here schema2's type is replaced with record and fields are same as schema1
        's
     */

    ObjectNode schema2Type = (ObjectNode) schema2.get("type");
    if (schema1.equals(schema2Type) && schema1.has("fields")) {
      schema2.set("type", schema1.get("type"));
      schema2.set("fields", schema1.get("fields"));
    }

  }

  private static void updateMergedArrays(ObjectNode schema1, ObjectNode schema2) {

    /*
    Update array schema2 to have items and type as schema1

    Restructuring Array items, similar to record
    Eg, schema1 {"name":"array","type":"array","items":"int"}
        schema2 {"name":"array","type":{"type":"array","items":"int"}}
        schema2 is changed to same as schema1
     */
    ObjectNode schema2Type = (ObjectNode) schema2.get("type");

    if (schema1.get("type").asText().equals("array") && schema2Type.has("items")) {

      if ((schema2Type.get("items") instanceof TextNode)
          && schema2Type.get("items").equals(schema1.get("items"))) {

        // 1D Array
        schema2.put("type", "array");
        schema2.set("items", schema1.get("items"));

      } else if ((schema2Type.get("items") instanceof ObjectNode)
          && schema2Type.get("items").equals(schema1.get("items"))) {

        // 2D or higher Array
        schema2.put("type", "array");
        schema2.set("items", schema1.get("items"));

      }

    }

  }

  public static void mergeUnion(ArrayList<ObjectNode> schemaList, boolean matchAll) {

    if (schemaList.size() == 0) {
      return;
    }

    for (ObjectNode curr : schemaList) {
      compressRecordToUnion(curr);
    }

    if (matchAll) {
      for (ObjectNode obj : schemaList) {
        mergeUnionLoops(schemaList, obj);
      }
    } else {
      mergeUnionLoops(schemaList, schemaList.get(0));
    }

  }


  private static void mergeUnionLoops(ArrayList<ObjectNode> schemaList, ObjectNode starting) {

    // Loop 1 : Match Schema 1 with rest and modify only first schema to include all union branches
    for (ObjectNode curr : schemaList) {
      if (!starting.equals(curr)) {
        matchArrayNode(starting, curr, false);
      }
    }

    // Loop 2 : Match Schema 1 with rest and check if second has corresponding branch in union
    // If yes, then copy union from schema 1 to current schema
    for (ObjectNode curr : schemaList) {
      if (!starting.equals(curr)) {
        matchArrayNode(starting, curr, true);
      }
    }

  }

  static void compressRecordToUnion(ObjectNode schema) {

    /*
    Compress Record with one field to type union if possible
    Eg, schema1 {name: length, type: {name: length, fields:{name:long, type:long}}
    Field can be interpreted of type unions with branch long
    Output for the function is {name: length, type:[long]}
    */

    ArrayList<String> unionBranches = checkForUnion(schema, false);
    if (unionBranches.size() == 1) {
      ArrayNode union = mapper.createArrayNode();
      union.add(unionBranches.get(0));
      if (schema.get("type").asText().equals("array")) {
        schema.put("items", unionBranches.get(0));
      } else {
        schema.set("type", union);
        schema.remove("fields");
      }
      return;
    }

    for (String typeName : Arrays.asList("type", "fields")) {
      if (schema.has(typeName) && schema.get(typeName) instanceof ArrayNode) {
        ArrayNode arr = (ArrayNode) schema.get(typeName);
        for (int i = 0; i < arr.size(); i++) {
          if (arr.get(i) instanceof ObjectNode) {
            compressRecordToUnion((ObjectNode) arr.get(i));
          }
        }
      }
    }

    if (schema.has("type") && schema.get("type") instanceof ObjectNode) {
      compressRecordToUnion((ObjectNode) schema.get("type"));
    }

  }

}
