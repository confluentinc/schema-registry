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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.Comparator;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema;
import io.confluent.kafka.schemaregistry.maven.derive.schema.protobuf.MergeProtoBufUtils;
import io.confluent.kafka.schemaregistry.maven.derive.schema.utils.MergeNumberUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeriveAvroSchema extends DeriveSchema {

  private static final Logger logger = LoggerFactory.getLogger(DeriveAvroSchema.class);
  final boolean strictCheck;
  final boolean typeProtoBuf;
  private static int currentMessage = -1;
  private static int depth = 0;

  public static int getCurrentMessage() {
    return currentMessage;
  }

  static void setCurrentMessage(int currentMessage) {
    DeriveAvroSchema.currentMessage = currentMessage;
  }

  public static int getDepth() {
    return depth;
  }

  public static void setDepth(int depth) {
    DeriveAvroSchema.depth = depth;
  }

  public DeriveAvroSchema(boolean isStrict) {
    this.strictCheck = isStrict;
    this.typeProtoBuf = false;
  }

  String errorMessageNoSchemasFound = "No strict schemas can be generated for the given messages. "
      + "Please try using the lenient version to generate a schema.";

  private static void checkValidSchema(ObjectNode schema) {
    AvroSchema avroschema = new AvroSchema(schema.toString());
    avroschema.validate();
    if (schema.get("fields").isEmpty()) {
      throw new IllegalArgumentException("Ignoring Empty record passed.");
    }
  }

  private List<ObjectNode> getSchemaForMultipleMessagesLenient(ArrayList<Object> messageObjects)
      throws JsonProcessingException {

    ObjectNode schema = DeriveAvroSchemaArray.getSchemaForArray(messageObjects,
        "Record", strictCheck, typeProtoBuf, false, true);
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
    ArrayList<ObjectNode> schemaList = DeriveAvroSchemaArray.getSchemaOfAllElements(messageObjects,
        "Record", true, false, true);

    ArrayList<ObjectNode> uniqueList = DeriveSchema.getUnique(schemaList);

    if (uniqueList.size() == 0) {
      logger.error(errorMessageNoSchemasFound);
      throw new IllegalArgumentException(errorMessageNoSchemasFound);
    }

    List<List<Integer>> schemaToMessagesInfo = MergeProtoBufUtils.getUniqueWithMessageInfo(
        schemaList, uniqueList, null);

    MergeNumberUtils.mergeNumberTypes(uniqueList, true);
    MergeUnionUtils.mergeUnion(uniqueList, true);

    ArrayList<ObjectNode> uniqueList2 = DeriveSchema.getUnique(uniqueList);
    schemaToMessagesInfo = MergeProtoBufUtils.getUniqueWithMessageInfo(
        uniqueList, uniqueList2, schemaToMessagesInfo);

    ArrayList<ObjectNode> schemaInformation = new ArrayList<>();
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
      for (Integer messageIndex : messagesMatched) {
        messagesMatchedArr.add(messageIndex);
      }
      schemaInfo.put("numMessagesMatched", schemaToMessagesInfo.get(i).size());
      schemaInformation.add(schemaInfo);
    }

    if (schemaInformation.size() == 0) {
      logger.error(errorMessageNoSchemasFound);
      throw new IllegalArgumentException(errorMessageNoSchemasFound);
    }

    Comparator<ObjectNode> comparator
        = Comparator.comparingInt(obj -> obj.get("numMessagesMatched").asInt());

    schemaInformation.sort(comparator.reversed());
    return schemaInformation;
  }

}