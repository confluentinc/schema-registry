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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;
import io.confluent.kafka.schemaregistry.maven.derive.schema.protobuf.DeriveProtobufSchema;
import io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema;
import io.confluent.kafka.schemaregistry.maven.derive.schema.utils.MergeNumberUtils;
import io.confluent.kafka.schemaregistry.maven.derive.schema.protobuf.MergeProtoBufUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema.mapper;

public class DeriveAvroSchemaArray {

  private static final Logger logger = LoggerFactory.getLogger(DeriveAvroSchema.class);


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
  public static ArrayList<ObjectNode> getSchemaOfAllElements(List<Object> messages,
                                                             String name,
                                                             boolean strictCheck,
                                                             boolean typeProtoBuf,
                                                             boolean multipleMessages)

      throws JsonProcessingException, IllegalArgumentException {

    ArrayList<ObjectNode> arr = new ArrayList<>();

    for (int i = 0; i < messages.size(); i++) {

      if (DeriveAvroSchema.getDepth() == 0) {
        DeriveAvroSchema.setCurrentMessage(i);
      }
      Object message = messages.get(i);
      Optional<ObjectNode> x = DeriveAvroSchemaPrimitive.getPrimitiveSchema(
          message, strictCheck, typeProtoBuf);

      DeriveAvroSchema.setDepth(DeriveAvroSchema.getDepth() + 1);

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
          List<Object> l = DeriveSchema.getListFromArray(message);
          arr.add(getSchemaForArray(l, name, strictCheck, typeProtoBuf));
        } else {
          ObjectNode objectNode = mapper.valueToTree(message);
          arr.add(DeriveAvroSchemaRecord.getSchemaForRecord(
              objectNode, name, strictCheck, typeProtoBuf, false));
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

      DeriveAvroSchema.setDepth(DeriveAvroSchema.getDepth() - 1);
    }

    return arr;
  }


  private static ObjectNode getModeForArray(ArrayList<ObjectNode> schemaList,
                                            ArrayList<String> schemaStrings) {

    int modeIndex = MergeProtoBufUtils.getMode(schemaStrings);
    int freq = Collections.frequency(schemaStrings, schemaStrings.get(modeIndex));
    if (freq != schemaStrings.size() && DeriveAvroSchema.getDepth() == 0) {
      logger.warn("Found multiple schemas for given messages. Choosing most occurring type");
    }
    return schemaList.get(modeIndex);

  }

  private static void checkForSameStructure(ArrayList<String> schemaStrings, String name,
                                            boolean throwError)
      throws IllegalArgumentException, JsonProcessingException {

    // Checking if all elements have the same structure for strict check.
    for (String schemaString : schemaStrings) {
      if (!schemaStrings.get(0).equals(schemaString)) {

        TypeReference<HashMap<String, Object>> mapType =
            new TypeReference<HashMap<String, Object>>() {};

        Map<String, Object> firstMap = mapper.readValue(schemaString, mapType);
        Map<String, Object> secondMap = mapper.readValue(schemaStrings.get(0), mapType);
        String diff = Maps.difference(firstMap, secondMap).toString();

        String errorMessage = String.format("Message %d: Array '%s' should have "
            + "all elements of the same type. %s",
            DeriveAvroSchema.getCurrentMessage(), name, diff);

        if (throwError) {
          logger.error(errorMessage);
          throw new IllegalArgumentException(errorMessage);
        } else if (DeriveAvroSchema.getDepth() > 0) {
          logger.warn(errorMessage);
          return;
        }
      }
    }

  }

  private static ArrayList<ObjectNode> getUniqueList(ArrayList<ObjectNode> schemaList,
                                                     boolean strictCheck) {

    /*
    To reduce computation, use only unique values for strict check
    Lenient check we need frequency of data type, hence reassign schemaList
    only if 1 value remains use unique
    */

    ArrayList<ObjectNode> uniqueSchemaList = DeriveSchema.getUnique(schemaList);
    if (uniqueSchemaList.size() == 1 || strictCheck) {
      return uniqueSchemaList;
    }
    return schemaList;

  }


  /**
   * Returns schema for array.
   * Performs merging of number types, merging of records for ProtoBuf
   * and merging of unions for Avro.
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
  public static ObjectNode getDatatypeForArray(List<Object> messages,
                                               String name,
                                               boolean strictCheck,
                                               boolean typeProtoBuf,
                                               boolean multipleMessages)
      throws JsonProcessingException, IllegalArgumentException {

    ArrayList<ObjectNode> schemaList = getSchemaOfAllElements(messages,
        name, strictCheck, typeProtoBuf, multipleMessages);

    schemaList = getUniqueList(schemaList, strictCheck);

    MergeNumberUtils.mergeNumberTypes(schemaList, !strictCheck);
    schemaList = getUniqueList(schemaList, strictCheck);

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


  public static ObjectNode getSchemaForArray(List<Object> field, String name, boolean strictCheck,
                                             boolean typeProtoBuf)
      throws JsonProcessingException {
    return getSchemaForArray(field, name, strictCheck, typeProtoBuf, false, false);
  }

  public static ObjectNode getSchemaForArray(List<Object> messages,
                                             String name,
                                             boolean strictCheck,
                                             boolean typeProtoBuf,
                                             boolean calledAsField,
                                             boolean multipleMessages)
      throws JsonProcessingException, IllegalArgumentException {

    ObjectNode schema = mapper.createObjectNode();

    if (typeProtoBuf) {
      schema.put("__type", "array");
    }

    schema.put("name", name);

    if (!calledAsField) {
      schema.put("type", "array");
    }

    ObjectNode elementSchema = getDatatypeForArray(messages, name, strictCheck,
        typeProtoBuf, multipleMessages);

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
      fillRecursiveType(elementSchema, schema, typeProtoBuf, calledAsField);

    }

    return schema;
  }


  private static void fillRecursiveType(ObjectNode element, ObjectNode schema, boolean typeProtoBuf,
                                        boolean calledAsField) {

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

}
