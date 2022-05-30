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
import io.confluent.kafka.schemaregistry.maven.derive.schema.utils.ReadFileUtils;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Main class to generate schema for provided input messages, schema type and strict flag.
 */
public class DeriveSchemaMain {

  /**
   * Derive schema for multiple messages based on schema type and strict flag
   * Calls 'getSchemaForMultipleMessages' for the schema type.
   *
   * @param schemaType  One of Avro, Json or ProtoBuf
   * @param strictCheck flag to specify strict check
   * @param messages    List of messages, each message is a JSONObject
   * @return List of JSONObjects, each object gives information of schema,
   *          and which messages it matches
   * @throws JsonProcessingException thrown if message not in JSON format
   */
  public static List<JSONObject> caseWiseOutput(String schemaType,
                                                boolean strictCheck,
                                                ArrayList<String> messages)
      throws JsonProcessingException {

    List<JSONObject> ans = new ArrayList<>();
    if (schemaType == null) {
      throw new IllegalArgumentException("Schema Type not set");
    }
    switch (schemaType.toLowerCase()) {
      case "avro": {

        DeriveAvroSchema schemaGenerator = new DeriveAvroSchema(strictCheck);
        List<JSONObject> schemas = schemaGenerator.getSchemaForMultipleMessages(messages);
        ans.addAll(schemas);
        break;
      }
      case "json": {

        DeriveJsonSchema schemaGenerator = new DeriveJsonSchema();
        JSONObject schema = schemaGenerator.getSchemaForMultipleMessages(messages);
        ans.add(schema);
        break;

      }
      case "protobuf":
        DeriveProtobufSchema schemaGenerator = new DeriveProtobufSchema(strictCheck);
        List<JSONObject> schemas = schemaGenerator.getSchemaForMultipleMessages(messages);
        ans.addAll(schemas);
        break;

      default:
        throw new IllegalArgumentException("Schema type not understood. "
            + "Use Avro, Json or Protobuf");

    }

    return ans;
  }

  /**
   * Function to generate schema for provided input messages, schema type and strict flag.
   *
   * @param args message/s location/s, schema type and strict flag
   * @throws IOException thrown if file not present
   */

  public static void main(String[] args) throws IOException {

    ArrayList<String> messages = new ArrayList<>();
    String schemaType;
    boolean strictCheck = false;

    int n = args.length;
    if (n > 1 && args[n - 1].equalsIgnoreCase("json")) {
      schemaType = args[n - 1];
      for (int i = 0; i < n - 1; i++) {
        File file = new File(args[i]);
        messages.addAll(ReadFileUtils.readMessagesToString(file));
      }
    } else if (n > 2) {
      strictCheck = Boolean.parseBoolean(args[n - 1]);
      schemaType = args[n - 2];
      for (int i = 0; i < n - 2; i++) {
        File file = new File(args[i]);
        messages.addAll(ReadFileUtils.readMessagesToString(file));
      }
    } else {
      System.err.println("Please provide message/s location/s, schema type and strict flag.");
      return;
    }

    System.out.println(caseWiseOutput(schemaType, strictCheck, messages));
  }


}


