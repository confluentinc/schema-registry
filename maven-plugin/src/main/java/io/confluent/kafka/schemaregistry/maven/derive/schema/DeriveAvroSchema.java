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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import java.lang.reflect.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.List;
import java.util.Collections;
import java.util.Comparator;

import com.google.gson.reflect.TypeToken;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.maven.derive.schema.utils.MergeProtoBufUtils;
import io.confluent.kafka.schemaregistry.maven.derive.schema.utils.MergeJsonUtils;
import io.confluent.kafka.schemaregistry.maven.derive.schema.utils.MergeNumberUtils;
import io.confluent.kafka.schemaregistry.maven.derive.schema.utils.MergeUnionUtils;

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
  private int depth = 0;

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

    classToDataType.put("class com.fasterxml.jackson.databind.node.DoubleNode", "double");
    classToDataType.put("class com.fasterxml.jackson.databind.node.TextNode", "string");
    classToDataType.put("class com.fasterxml.jackson.databind.node.BigIntegerNode", "double");

    if (this.typeProtoBuf) {
      fillProtoBufMap();
    } else {
      fillAvroMap();
    }

  }

  private void fillProtoBufMap() {
    classToDataType.put("class com.fasterxml.jackson.databind.node.IntNode", "int32");
    classToDataType.put("class com.fasterxml.jackson.databind.node.LongNode", "int64");
    classToDataType.put("class com.fasterxml.jackson.databind.node.BooleanNode", "bool");
    classToDataType.put("class com.fasterxml.jackson.databind.node.NullNode",
        "google.protobuf.Any");
  }

  private void fillAvroMap() {
    classToDataType.put("class com.fasterxml.jackson.databind.node.IntNode", "int");
    classToDataType.put("class com.fasterxml.jackson.databind.node.LongNode", "long");
    classToDataType.put("class com.fasterxml.jackson.databind.node.BooleanNode", "boolean");
    classToDataType.put("class com.fasterxml.jackson.databind.node.NullNode", "null");
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
   * @return ObjectNode if type primitive else empty option
   */
  Optional<ObjectNode> getPrimitiveSchema(Object field) throws IllegalArgumentException,
      JsonProcessingException {

    String jsonInferredType;

    if (field == null) {
      jsonInferredType = "class com.fasterxml.jackson.databind.node.NullNode";
    } else {
      jsonInferredType = field.getClass().toString();
    }

    if (jsonInferredType.equals("class com.fasterxml.jackson.databind.node.BigIntegerNode")) {
      if (this.strictCheck) {
        String errorMessage = String.format("Message %d: Numeric value %s out of range of long "
                + "(-9223372036854775808 9223372036854775807). "
                + "Change value to string type or change value to inside the range of long",
            currentMessage, field);
        logger.error(errorMessage);
        throw new IllegalArgumentException(errorMessage);
      } else {
        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.put("type", "double");
        logger.warn(String.format("Message %d: Value out of range of long. "
            + "Mapping to double", currentMessage));
        return Optional.of(objectNode);
      }
    }

    if (classToDataType.containsKey(jsonInferredType)) {
      String schemaString = String.format("{\"type\" : \"%s\"}",
          classToDataType.get(jsonInferredType));
      return Optional.of(mapper.readValue(schemaString, ObjectNode.class));
    }

    return Optional.empty();
  }

  /**
   * Return a list of schemas as ObjectNode, a schema corresponding to each message.
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
   * @param messages         List of messages, each message is a ObjectNode
   * @param name             Name of Element
   * @param multipleMessages Flag to denote multiple message
   * @return List of Schemas, one for each message
   * @throws JsonProcessingException  thrown if message not in JSON format
   * @throws IllegalArgumentException thrown if no messages can be generated for strict check
   */
  public ArrayList<ObjectNode> getSchemaOfAllElements(List<Object> messages, String name,
                                                      boolean multipleMessages)
      throws JsonProcessingException, IllegalArgumentException {

    ArrayList<ObjectNode> arr = new ArrayList<>();

    for (int i = 0; i < messages.size(); i++) {

      if (depth == 0) {
        setCurrentMessage(i);
      }
      Object message = messages.get(i);
      Optional<ObjectNode> x = getPrimitiveSchema(message);

      depth++;

      try {
        if (x.isPresent()) {
          if (x.get().get("type").asText().equals("null")) {
            ObjectNode nullType = mapper.createObjectNode();
            nullType.put("name", name);
            nullType.put("type", "null");
            arr.add(nullType);
          } else {
            arr.add(x.get());
          }
        } else if (message instanceof ArrayNode) {
          List<Object> l = getListFromArray(message);
          arr.add(getSchemaForArray(l, name));
        } else {
          ObjectNode objectNode = mapper.valueToTree(message);
          arr.add(getSchemaForRecord(objectNode, name, false));
        }

      } catch (IllegalArgumentException e) {
        if (multipleMessages) {
          // Empty Object added to maintain correct index
          arr.add(mapper.createObjectNode());
          logger.warn(String.format("Message %d: cannot find Strict schema. "
              + "Hence, ignoring for multiple messages.", i));
        } else {
          logger.error(e.getMessage());
          throw new IllegalArgumentException(e.getMessage());
        }
      }

      depth--;
    }

    return arr;
  }

  public ArrayList<ObjectNode> getSchemaOfAllElements(List<Object> list, String name)
      throws JsonProcessingException {
    return getSchemaOfAllElements(list, name, false);
  }

  /**
   * Returns highest occurring datatype. If all elements have same occurrences, first is returned
   * <p>
   * Prints warning in case there are multiple types of elements. Expected one data type
   * </p>
   *
   * @param schemaList    List of schemas as ObjectNode
   * @param schemaStrings List of schema as Strings
   */

  ObjectNode getModeForArray(ArrayList<ObjectNode> schemaList, ArrayList<String> schemaStrings) {

    int modeIndex = MergeProtoBufUtils.getMode(schemaStrings);
    int freq = Collections.frequency(schemaStrings, schemaStrings.get(modeIndex));
    if (freq != schemaStrings.size() && depth == 0) {
      logger.warn("Found multiple schemas for given messages. Choosing most occurring type");
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
  private void checkForSameStructure(ArrayList<String> schemaStrings, String name,
                                     boolean throwError)
      throws IllegalArgumentException {

    for (String schemaString : schemaStrings) {
      if (!schemaStrings.get(0).equals(schemaString)) {

        Type mapType = new TypeToken<Map<String, Object>>() {
        }.getType();
        Map<String, Object> firstMap = gson.fromJson(schemaString, mapType);
        Map<String, Object> secondMap = gson.fromJson(schemaStrings.get(0), mapType);
        String diff = Maps.difference(firstMap, secondMap).toString().substring(30);

        String errorMessage = String.format("Message %d: Array '%s' should have "
            + "all elements of the same type. Difference is : %s", currentMessage, name, diff);
        if (throwError) {
          logger.error(errorMessage);
          throw new IllegalArgumentException(errorMessage);
        } else if (depth > 0) {
          logger.warn(errorMessage);
          return;
        }
      }
    }

  }

  private ArrayList<ObjectNode> getUniqueList(ArrayList<ObjectNode> schemaList) {

    /*
    To reduce computation, use only unique values for strict check
    Lenient check we need frequency of data type, hence reassign schemaList
    only if 1 value remains use unique
    */

    ArrayList<ObjectNode> uniqueSchemaList = MergeJsonUtils.getUnique(schemaList);
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
   * @param messages         List of messages, each message is a ObjectNode
   * @param name             name used for record and arrays
   * @param multipleMessages flag to specify multiple messages
   * @return map with information about schema
   * @throws JsonProcessingException  thrown if message not in JSON format
   * @throws IllegalArgumentException thrown if schema cannot be found in strict version
   */
  public ObjectNode getDatatypeForArray(List<Object> messages,
                                        String name,
                                        boolean multipleMessages)
      throws JsonProcessingException, IllegalArgumentException {

    ArrayList<ObjectNode> schemaList = getSchemaOfAllElements(messages,
        name, multipleMessages);

    schemaList = getUniqueList(schemaList);

    MergeNumberUtils.mergeNumberTypes(schemaList, !strictCheck);
    schemaList = getUniqueList(schemaList);

    if (typeProtoBuf) {
      // Merge Records for ProtoBuf
      MergeProtoBufUtils.mergeRecordsInsideArray(schemaList, strictCheck);
    } else {
      // Merge Unions for Avro
      MergeUnionUtils.mergeUnion(schemaList, !strictCheck);
    }

    ArrayList<String> schemaStrings = new ArrayList<>();
    for (ObjectNode objectNode : schemaList) {
      schemaStrings.add(objectNode.toString());
    }

    if (!strictCheck) {
      // Lenient check choose highest occurring schema
      if (schemaList.size() > 0) {
        checkForSameStructure(schemaStrings, name, false);
        return getModeForArray(schemaList, schemaStrings);
      }
      return mapper.createObjectNode();
    }

    // Check if all schemas are same, else raise error
    checkForSameStructure(schemaStrings, name, true);
    if (schemaList.size() > 0) {
      return schemaList.get(0);
    }
    return mapper.createObjectNode();
  }


  public ObjectNode getSchemaForArray(List<Object> field, String name)
      throws JsonProcessingException {
    return getSchemaForArray(field, name, false, false);
  }

  /**
   * Generates schema for array type.
   *
   * @param messages         List of messages, each message is a ObjectNode
   * @param name             name assigned to array
   * @param calledAsField    flag to check if called as a field
   * @param multipleMessages flag to specify multiple messages
   * @return schema
   * @throws JsonProcessingException  thrown if message not in JSON format
   * @throws IllegalArgumentException thrown if schema cannot be found in strict version
   */

  public ObjectNode getSchemaForArray(List<Object> messages, String name,
                                      boolean calledAsField, boolean multipleMessages)
      throws JsonProcessingException, IllegalArgumentException {

    ObjectNode schema = mapper.createObjectNode();

    if (typeProtoBuf) {
      schema.put("__type", "array");
    }

    schema.put("name", name);

    if (!calledAsField) {
      schema.put("type", "array");
    }

    ObjectNode elementSchema = getDatatypeForArray(messages, name, multipleMessages);

    if (!elementSchema.isEmpty()) {
      DeriveProtobufSchema.checkForArrayOfNull(typeProtoBuf, elementSchema);
    } else {
      elementSchema.set("type", mapper.createArrayNode());
    }

    if (!elementSchema.has("name")) {

      if (typeProtoBuf) {
        elementSchema.put("__type", "array");
      }

      elementSchema.set("items", elementSchema.get("type"));
      elementSchema.put("type", "array");

      if (calledAsField) {
        schema.set("type", elementSchema);
      } else {
        schema.set("items", elementSchema.get("items"));
      }

    } else {

      DeriveProtobufSchema.checkFor2dArrays(typeProtoBuf, elementSchema);
      fillRecursiveType(elementSchema, schema, calledAsField);

    }

    return schema;
  }

  private void fillRecursiveType(ObjectNode element, ObjectNode schema, boolean calledAsField) {

    if (calledAsField) {

      ObjectNode type = mapper.createObjectNode();

      type.put("type", "array");

      if (element.has("type") && element.get("type") instanceof ArrayNode) {
        // Element is of type union
        type.set("items", element.get("type"));
      } else {
        type.set("items", element);
      }

      if (typeProtoBuf) {
        type.put("__type", "array");
      }
      schema.set("type", type);

    } else {
      schema.set("items", element);
    }
  }

  public ObjectNode getSchemaForRecord(ObjectNode objectNode, String name)
      throws JsonProcessingException {
    return getSchemaForRecord(objectNode, name, false);
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
  public ObjectNode getSchemaForRecord(ObjectNode message, String name,
                                       boolean calledAsField) throws JsonProcessingException {

    ObjectNode schema = mapper.createObjectNode();

    if (typeProtoBuf) {
      schema.put("__type", "record");
    }

    schema.put("name", name);

    if (!calledAsField) {
      schema.put("type", "record");
    }

    schema.set("fields", mapper.createArrayNode());

    for (String key : getSortedKeys(message)) {

      checkName(key);
      Object field = message.get(key);
      Optional<ObjectNode> primitiveSchema = getPrimitiveSchema(field);

      if (primitiveSchema.isPresent()) {

        ArrayNode fields = (ArrayNode) schema.get("fields");
        ObjectNode primitiveType = mapper.createObjectNode();
        primitiveType.put("name", key);
        primitiveType.set("type", primitiveSchema.get().get("type"));
        fields.add(primitiveType);

      } else {

        ObjectNode info;
        if (field instanceof ArrayNode) {
          info = getSchemaForArray(getListFromArray(field), key, true, false);
        } else {
          info = getSchemaForRecord(mapper.valueToTree(field), key, true);
        }

        ArrayNode fields = (ArrayNode) schema.get("fields");
        fields.add(info);

      }
    }

    if (calledAsField) {
      ObjectNode recursive = mapper.createObjectNode();
      recursive.put("name", name);
      recursive.set("fields", schema.get("fields"));
      recursive.put("type", "record");

      if (typeProtoBuf) {
        recursive.put("__type", "record");
      }

      schema.remove("fields");
      schema.set("type", recursive);
    }

    return schema;
  }

  private static void checkValidSchema(ObjectNode schema) {
    AvroSchema avroschema = new AvroSchema(schema.toString());
    avroschema.validate();
    if (schema.get("fields").isEmpty()) {
      throw new IllegalArgumentException("Ignoring Empty record passed.");
    }
  }

  private List<ObjectNode> getSchemaForMultipleMessagesLenient(ArrayList<Object> messageObjects)
      throws JsonProcessingException {

    ObjectNode schema = getSchemaForArray(messageObjects, "Record",
        false, true);
    ObjectNode finalSchema = (ObjectNode) schema.get("items");

    try {
      checkValidSchema(finalSchema);
    } catch (Exception e) {
      String errorMessage = "Unable to find schema. " + e.getMessage();
      logger.warn(errorMessage);
      throw e;
    }

    ObjectNode schemaInfo = mapper.createObjectNode();
    schemaInfo.set("schema", finalSchema);
    return Collections.singletonList(schemaInfo);

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
  public List<ObjectNode> getSchemaForMultipleMessages(List<String> messages)
      throws JsonProcessingException {

    ArrayList<Object> messageObjects = new ArrayList<>();
    for (String message : messages) {
      messageObjects.add(mapper.readTree(message));
    }

    /*
    Lenient check returns 1 schema Picking highest occurring datatype in case of conflicts
    */

    if (!strictCheck) {
      return getSchemaForMultipleMessagesLenient(messageObjects);
    }

    /*
    Strict schema for each element is found
    Merging of Unions is performed then
    All Unique Schemas are returned and the messages it matches
    */

    ArrayList<ObjectNode> schemaList = getSchemaOfAllElements(messageObjects,
        "Record", true);

    ArrayList<ObjectNode> uniqueList = MergeJsonUtils.getUnique(schemaList);

    if (uniqueList.size() == 0) {
      logger.error(errorMessageNoSchemasFound);
      throw new IllegalArgumentException(errorMessageNoSchemasFound);
    }

    List<List<Integer>> schemaToMessagesInfo = MergeProtoBufUtils.getUniqueWithMessageInfo(
        schemaList, uniqueList, null);

    MergeNumberUtils.mergeNumberTypes(uniqueList, true);
    MergeUnionUtils.mergeUnion(uniqueList, true);

    ArrayList<ObjectNode> uniqueList2 = MergeJsonUtils.getUnique(uniqueList);
    schemaToMessagesInfo = MergeProtoBufUtils.getUniqueWithMessageInfo(
        uniqueList, uniqueList2, schemaToMessagesInfo);

    ArrayList<ObjectNode> ans = new ArrayList<>();
    for (int i = 0; i < uniqueList2.size(); i++) {
      ObjectNode schemaInfo = mapper.createObjectNode();
      List<Integer> messagesMatched = schemaToMessagesInfo.get(i);

      try {
        checkValidSchema(uniqueList2.get(i));
      } catch (Exception e) {
        String errorMessage = String.format("Messages %s: Unable to find schema. ", messagesMatched)
            + e.getMessage();
        logger.warn(errorMessage);
        continue;
      }

      schemaInfo.set("schema", uniqueList2.get(i));
      Collections.sort(messagesMatched);
      ArrayNode messagesMatchedArr = schemaInfo.putArray("messagesMatched");
      for (Integer m : messagesMatched) {
        messagesMatchedArr.add(m);
      }
      schemaInfo.put("numMessagesMatched", schemaToMessagesInfo.get(i).size());
      ans.add(schemaInfo);
    }

    if (ans.size() == 0) {
      logger.error(errorMessageNoSchemasFound);
      throw new IllegalArgumentException(errorMessageNoSchemasFound);
    }

    Comparator<ObjectNode> comparator
        = Comparator.comparingInt(obj -> obj.get("numMessagesMatched").asInt());

    ans.sort(comparator.reversed());
    return ans;
  }


}