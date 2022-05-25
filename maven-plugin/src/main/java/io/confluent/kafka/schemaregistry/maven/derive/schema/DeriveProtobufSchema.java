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
import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.confluent.kafka.schemaregistry.maven.derive.schema.utils.MergeAvroProtoBufUtils;
import io.confluent.kafka.schemaregistry.maven.derive.schema.utils.MergeJsonUtils;
import io.confluent.kafka.schemaregistry.maven.derive.schema.utils.MapAndArray;
import org.json.JSONArray;
import org.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class with functionality to derive schema from messages for JSON.
 */
public class DeriveProtobufSchema {

  private static final Logger logger = LoggerFactory.getLogger(DeriveProtobufSchema.class);
  final boolean strictCheck;

  String errorMessageNoSchemasFound = "No strict schemas can be generated for the given messages. "
      + "Please try using the lenient version to generate a schema.";

  static void checkFor2dArrays(boolean typeProtoBuf, JSONObject element) {

    if (typeProtoBuf && element.get("__type").equals("array")) {
      logger.error("Protobuf doesn't support array of arrays.");
      throw new IllegalArgumentException("Protobuf doesn't support array of arrays.");
    }

  }

  static void checkForArrayOfNull(boolean typeProtoBuf, JSONObject element) {

    if (typeProtoBuf && element.has("type")
        && element.get("type").equals("google.protobuf.Any")) {
      throw new IllegalArgumentException("Protobuf support doesn't array of null.");
    }

  }

  static void checkValidSchema(String message, ProtobufSchema schema)
      throws InvalidProtocolBufferException {
    String formattedString = new JSONObject(message).toString();
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
  public String avroSchemaToProtobufSchema(JSONObject avroSchema, int fieldNum, String name) {

    StringBuilder ans = new StringBuilder();
    // Type array
    if (avroSchema.get("__type").equals("array")) {

      if (avroSchema.get("items") instanceof String) {
        return String.format("repeated %s %s = %d;",
            avroSchema.get("items"), name, fieldNum) + '\n';
      } else {
        if (avroSchema.get("items") instanceof JSONArray) {
          ans.append(
              String.format("repeated %s %s = %d;%n", "google.protobuf.Any", name, fieldNum));
          return ans.toString();
        }
        String fieldDefinition = avroSchemaToProtobufSchema((JSONObject) avroSchema.get("items"),
            fieldNum, name + "Message");
        ans.append(
            String.format("repeated %s %s = %d;%n", name + "Message", name, fieldNum));
        ans.append(fieldDefinition);
        return ans.toString();
      }

    }

    ans.append(String.format("message %s { %n", name));
    JSONArray fields = (JSONArray) avroSchema.get("fields");

    for (int i = 0; i < fields.length(); i++) {
      JSONObject obj = (JSONObject) (fields.get(i));

      if (obj.get("type") instanceof String) {
        ans.append(String.format("  %s %s = %d;", obj.get("type"), obj.get("name"), fieldNum++));
        ans.append('\n');
      } else if (obj.get("__type").equals("record")) {
        JSONObject j = (JSONObject) obj.get("type");
        String newMessage = avroSchemaToProtobufSchema(j, 1, obj.get("name") + "Message");
        ans.append(newMessage);
        ans.append(String.format("  %s %s = %d; %n", obj.get("name") + "Message", obj.get("name"),
            fieldNum++));
      } else if (obj.get("__type").equals("array")) {
        JSONObject j = (JSONObject) obj.get("type");
        String newMessage = avroSchemaToProtobufSchema(j, fieldNum++, obj.get("name").toString());
        ans.append(newMessage);
      }
    }

    ans.append("}\n");
    return ans.toString();

  }

  ProtobufSchema getSchema(String message, String name) throws JsonProcessingException {

    JSONObject messageObject = new JSONObject(message);
    DeriveAvroSchema schemaGenerator = new DeriveAvroSchema(this.strictCheck, true);
    JSONObject schema = schemaGenerator.getSchemaForRecord(messageObject, name);
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

  private ProtobufSchema getSchemaFromAvro(String schema, String name) {
    String protobufString = avroSchemaToProtobufSchema(new JSONObject(schema), 1, name);
    return schemaStringToProto(protobufString);
  }


  /**
   * Get schema for multiple messages.
   * <p>
   * For lenient check, treated same as array of records and exactly one schema is returned
   * </p>
   * <p>
   * For Strict check, the schemas are merged for number types and record types.
   * Top 3 occurring schemas are returned
   * </p>
   *
   * @param messages list of messages, each message is a string
   * @return map with schema and the number of messages it matches
   * @throws JsonProcessingException  thrown if message not in JSON format
   * @throws IllegalArgumentException thrown if no messages can be generated for strict check
   */
  public List<JSONObject> getSchemaForMultipleMessages(List<String> messages)
      throws JsonProcessingException, IllegalArgumentException {

    ArrayList<Object> messageObjects = new ArrayList<>();
    for (String message : messages) {
      messageObjects.add(new JSONObject(message));
    }

    DeriveAvroSchema schemaGenerator = new DeriveAvroSchema(this.strictCheck, true);

    /*
    Lenient check returns 1 schema Picking highest occurring datatype in case of conflicts
    */

    if (!strictCheck) {
      JSONObject schema = schemaGenerator.getSchemaForArray(messageObjects, "MainMessage");
      JSONObject finalSchema = schema.getJSONObject("items");
      ProtobufSchema protobufSchema = schemaStringToProto(
          avroSchemaToProtobufSchema(finalSchema, 1, "MainMessage"));
      JSONObject schemaInfo = new JSONObject();
      schemaInfo.put("schema", protobufSchema);
      return Collections.singletonList(schemaInfo);
    }

    /*
    Strict schema for each element is found
    Merging of Records is performed then
    All Unique Schemas are returned and the messages it matches
    */

    ArrayList<JSONObject> schemaList = schemaGenerator.getSchemaOfAllElements(messageObjects,
        "Record", true);

    ArrayList<JSONObject> uniqueList = MergeJsonUtils.getUnique(schemaList);

    if (uniqueList.size() == 0) {
      logger.error(errorMessageNoSchemasFound);
      throw new IllegalArgumentException(errorMessageNoSchemasFound);
    }

    List<List<Integer>> schemaToMessagesInfo = MergeAvroProtoBufUtils.getUniqueWithMessageInfo(
        schemaList, uniqueList, null);


    MergeAvroProtoBufUtils.mergeNumberTypes(uniqueList, true);
    MapAndArray schemasAndMap = MergeAvroProtoBufUtils.tryAndMergeStrictFromList(
        uniqueList, schemaToMessagesInfo);

    ArrayList<JSONObject> schemas = schemasAndMap.getSchemas();
    List<List<Integer>> schemaToMessagesInfo2 = schemasAndMap.getSchemaToMessagesInfo();

    ArrayList<JSONObject> ans = new ArrayList<>();

    for (int i = 0; i < schemas.size(); i++) {
      JSONObject schemaInfo = new JSONObject();
      List<Integer> messagesMatched = schemaToMessagesInfo2.get(i);

      try {
        ProtobufSchema schema = getSchemaFromAvro(schemas.get(i).toString(), "Record");
        schemaInfo.put("schema", schema);
        checkValidSchema(messages.get(messagesMatched.get(0)), schema);
      } catch (Exception e) {
        String errorMessage = String.format("Messages %s: Unable to find schema", messagesMatched)
            + e.getMessage();
        logger.warn(errorMessage);
        continue;
      }
      Collections.sort(messagesMatched);
      schemaInfo.put("messagesMatched", messagesMatched);
      schemaInfo.put("numMessagesMatched", messagesMatched.size());
      ans.add(schemaInfo);
    }

    if (ans.size() == 0) {
      logger.error(errorMessageNoSchemasFound);
      throw new IllegalArgumentException(errorMessageNoSchemasFound);
    }

    return ans;
  }


}