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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema;
import io.confluent.kafka.schemaregistry.maven.derive.schema.avro.DeriveAvroSchemaArray;
import io.confluent.kafka.schemaregistry.maven.derive.schema.avro.DeriveAvroSchemaRecord;
import io.confluent.kafka.schemaregistry.maven.derive.schema.utils.MergeNumberUtils;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema.mapper;

public class DeriveProtobufSchema {

  private static final Logger logger = LoggerFactory.getLogger(DeriveProtobufSchema.class);
  final boolean strictCheck;

  String errorMessageNoSchemasFound = "No strict schemas can be generated for the given messages. "
      + "Please try using the lenient version to generate a schema.";

  public static void checkFor2dArrays(boolean typeProtoBuf, ObjectNode element) {

    if (typeProtoBuf && element.get("__type").asText().equals("array")) {
      logger.error("Protobuf doesn't support array of arrays.");
      throw new IllegalArgumentException("Protobuf doesn't support array of arrays.");
    }
  }

  public static void checkForArrayOfNull(boolean typeProtoBuf, ObjectNode element) {

    if (typeProtoBuf && element.has("type")
        && element.get("type").asText().equals("google.protobuf.Any")) {
      throw new IllegalArgumentException("Protobuf support doesn't array of null.");
    }
  }

  static void checkValidSchema(String message, ProtobufSchema schema)
      throws InvalidProtocolBufferException, JsonProcessingException {
    schema.validate();
    String formattedString = mapper.readTree(message).toString();
    ProtobufSchemaUtils.toObject(formattedString, schema);
  }

  public DeriveProtobufSchema(boolean isStrict) {
    this.strictCheck = isStrict;
  }

  /**
   * Converts generated Avro schema to ProtoBuf.
   *
   * @param avroSchema Avro schema to convert to ProtoBuf
   * @param fieldNum   Field number assigned to each field in ProtoBuf
   * @param name       name of Message
   * @return ProtoBuf schema as a string
   */
  public String avroSchemaToProtobufSchema(ObjectNode avroSchema, int fieldNum, String name) {

    StringBuilder protobufSchema = new StringBuilder();
    // Type array
    if (avroSchema.get("__type").asText().equals("array")) {

      if (avroSchema.get("items") instanceof TextNode) {
        return String.format("repeated %s %s = %d;",
            avroSchema.get("items").asText(), name, fieldNum) + '\n';
      } else {
        if (avroSchema.get("items") instanceof ArrayNode) {
          protobufSchema.append(
              String.format("repeated %s %s = %d;%n", "google.protobuf.Any", name, fieldNum));
          return protobufSchema.toString();
        }
        String fieldDefinition = avroSchemaToProtobufSchema((ObjectNode) avroSchema.get("items"),
            fieldNum, name + "Message");
        protobufSchema.append(
            String.format("repeated %s %s = %d;%n", name + "Message", name, fieldNum));
        protobufSchema.append(fieldDefinition);
        return protobufSchema.toString();
      }

    }

    protobufSchema.append(String.format("message %s { %n", name));
    ArrayNode fields = (ArrayNode) avroSchema.get("fields");

    for (int i = 0; i < fields.size(); i++) {
      ObjectNode obj = (ObjectNode) (fields.get(i));

      if (obj.get("type") instanceof TextNode) {
        protobufSchema.append(String.format("  %s %s = %d;", obj.get("type").asText(),
            obj.get("name").asText(), fieldNum++));
        protobufSchema.append('\n');
      } else if (obj.get("__type").asText().equals("record")) {
        ObjectNode j = (ObjectNode) obj.get("type");
        String newMessage = avroSchemaToProtobufSchema(j, 1, obj.get("name").asText() + "Message");
        protobufSchema.append(newMessage);
        protobufSchema.append(String.format("  %s %s = %d; %n", obj.get("name").asText()
            + "Message", obj.get("name").asText(), fieldNum++));
      } else if (obj.get("__type").asText().equals("array")) {
        ObjectNode j = (ObjectNode) obj.get("type");
        String newMessage = avroSchemaToProtobufSchema(j, fieldNum++, obj.get("name").asText());
        protobufSchema.append(newMessage);
      }
    }

    protobufSchema.append("}\n");
    return protobufSchema.toString();
  }

  ProtobufSchema getSchema(String message, String name) throws JsonProcessingException {

    ObjectNode messageObject = (ObjectNode) mapper.readTree(message);
    ObjectNode schema = DeriveAvroSchemaRecord.getSchemaForRecord(messageObject, name,
        this.strictCheck, true);
    String protobufString = avroSchemaToProtobufSchema(schema, 1, name);
    return schemaStringToProto(protobufString);
  }

  public ProtobufSchema getSchema(String message)
      throws JsonProcessingException {
    return getSchema(message, "mainMessage");
  }

  private ProtobufSchema schemaStringToProto(String schema) {

    if (schema.contains("google.protobuf.Any")) {
      String additionalInfo = "import \"google/protobuf/any.proto\";\n";
      String schemaString = "syntax = \"proto3\";\n\n" + additionalInfo + schema;
      return new ProtobufSchema(schemaString);
    }

    String schemaString = "syntax = \"proto3\";\n\n" + schema;
    return new ProtobufSchema(schemaString);
  }

  private ProtobufSchema getSchemaFromAvro(String schema, String name)
      throws JsonProcessingException {
    String protobufString = avroSchemaToProtobufSchema((ObjectNode) mapper.readTree(schema),
        1, name);
    return schemaStringToProto(protobufString);
  }

  public List<ObjectNode> getSchemaForMultipleMessages(List<String> messages)
      throws JsonProcessingException, IllegalArgumentException {

    ArrayList<Object> messageObjects = new ArrayList<>();
    for (String message : messages) {
      messageObjects.add(mapper.readTree(message));
    }

    /*
    Lenient check returns 1 schema Picking highest occurring datatype in case of conflicts
    */
    if (!strictCheck) {

      ObjectNode schema = DeriveAvroSchemaArray.getSchemaForArray(messageObjects, "Record",
          false, true, false, true);
      ObjectNode finalSchema = (ObjectNode) schema.get("items");

      try {
        ProtobufSchema protobufSchema = schemaStringToProto(
            avroSchemaToProtobufSchema(finalSchema, 1, "Record"));
        protobufSchema.validate();
        ObjectNode schemaInfo = mapper.createObjectNode();
        schemaInfo.put("schema", String.valueOf(protobufSchema));
        return Collections.singletonList(schemaInfo);
      } catch (Exception e) {
        String errorMessage = "Unable to find schema. " + e.getMessage();
        logger.warn(errorMessage);
        throw e;
      }

    }

    /*
    Strict schema for each element is found
    Merging of Records is performed then
    All Unique Schemas are returned and the messages it matches
    */
    ArrayList<ObjectNode> schemaList = DeriveAvroSchemaArray.getSchemaOfAllElements(messageObjects,
        "Record", true, true, true);

    ArrayList<ObjectNode> uniqueList = DeriveSchema.getUnique(schemaList);

    if (uniqueList.size() == 0) {
      logger.error(errorMessageNoSchemasFound);
      throw new IllegalArgumentException(errorMessageNoSchemasFound);
    }

    List<List<Integer>> schemaToMessagesInfo = MergeProtoBufUtils.getUniqueWithMessageInfo(
        schemaList, uniqueList, null);

    MergeNumberUtils.mergeNumberTypes(uniqueList, true);
    MapAndArray schemasAndMap = MergeProtoBufUtils.tryAndMergeStrictFromList(
        uniqueList, schemaToMessagesInfo);

    ArrayList<ObjectNode> schemas = schemasAndMap.getSchemas();
    List<List<Integer>> schemaToMessagesInfo2 = schemasAndMap.getSchemaToMessagesInfo();

    ArrayList<ObjectNode> schemaInformation = new ArrayList<>();

    for (int i = 0; i < schemas.size(); i++) {
      ObjectNode schemaInfo = mapper.createObjectNode();
      List<Integer> messagesMatched = schemaToMessagesInfo2.get(i);

      try {
        ProtobufSchema schema = getSchemaFromAvro(schemas.get(i).toString(), "Record");
        schemaInfo.put("schema", schema.canonicalString());
        checkValidSchema(messages.get(messagesMatched.get(0)), schema);
      } catch (Exception e) {
        String errorMessage = String.format("Messages %s: Unable to find schema. ", messagesMatched)
            + e.getMessage();
        logger.warn(errorMessage);
        continue;
      }
      Collections.sort(messagesMatched);
      ArrayNode messagesMatchedArr = schemaInfo.putArray("messagesMatched");
      for (Integer messageIndex : messagesMatched) {
        messagesMatchedArr.add(messageIndex);
      }
      schemaInfo.put("numMessagesMatched", messagesMatched.size());
      schemaInformation.add(schemaInfo);
    }

    if (schemaInformation.size() == 0) {
      logger.error(errorMessageNoSchemasFound);
      throw new IllegalArgumentException(errorMessageNoSchemasFound);
    }

    return schemaInformation;
  }

}