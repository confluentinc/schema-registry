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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.maven.derive.schema.avro.DeriveAvroSchema;
import io.confluent.kafka.schemaregistry.maven.derive.schema.json.DeriveJsonSchema;
import io.confluent.kafka.schemaregistry.maven.derive.schema.protobuf.DeriveProtobufSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public abstract class DeriveSchema {

  public static final ObjectMapper mapper = new ObjectMapper();

  public static ArrayList<ObjectNode> getUnique(ArrayList<ObjectNode> schemas) {

    Set<ObjectNode> setWithWrappedObjects = new HashSet<>(schemas);
    ArrayList<ObjectNode> uniqueList = new ArrayList<>();

    for (ObjectNode objectNode : setWithWrappedObjects) {
      if (objectNode != null && !objectNode.isEmpty()) {
        uniqueList.add(objectNode);
      }
    }

    return uniqueList;
  }

  public static List<Object> getListFromArray(Object field) {

    if (field instanceof ArrayNode) {
      ArrayNode arrayField = (ArrayNode) field;
      List<Object> objectList = new ArrayList<>();
      for (JsonNode fieldItem : arrayField) {
        objectList.add(fieldItem);
      }
      return objectList;
    }

    return ((List<Object>) field);
  }

  public static List<String> getSortedKeys(ObjectNode message) {

    ArrayList<String> keys = new ArrayList<>();
    for (Iterator<String> it = message.fieldNames(); it.hasNext(); ) {
      keys.add(it.next());
    }
    Collections.sort(keys);
    return keys;
  }

  /**
   * Derive schema for multiple messages based on schema type and strict flag
   * Calls 'getSchemaForMultipleMessages' for the schema type.
   *
   * @param schemaType  One of Avro, Json or ProtoBuf
   * @param strictCheck flag to specify strict check
   * @param messages    List of messages, each message is a ObjectNode
   * @return List of ObjectNode, each object gives information of schema,
   *          and which messages it matches
   * @throws JsonProcessingException thrown if message not in JSON format
   */
  public static List<ObjectNode> getSchemaInformation(String schemaType,
                                                      boolean strictCheck,
                                                      ArrayList<String> messages)
      throws JsonProcessingException {

    List<ObjectNode> schemaList = new ArrayList<>();
    if (schemaType == null) {
      throw new IllegalArgumentException("Schema Type not set");
    }
    switch (schemaType.toLowerCase()) {
      case "avro": {

        DeriveAvroSchema schemaGenerator = new DeriveAvroSchema(strictCheck);
        List<ObjectNode> schemas = schemaGenerator.getSchemaForMultipleMessages(messages);
        schemaList.addAll(schemas);
        break;
      }
      case "json": {

        DeriveJsonSchema schemaGenerator = new DeriveJsonSchema();
        ObjectNode schema = schemaGenerator.getSchemaForMultipleMessages(messages);
        schemaList.add(schema);
        break;

      }
      case "protobuf":
        DeriveProtobufSchema schemaGenerator = new DeriveProtobufSchema(strictCheck);
        List<ObjectNode> schemas = schemaGenerator.getSchemaForMultipleMessages(messages);
        schemaList.addAll(schemas);
        break;

      default:
        throw new IllegalArgumentException("Schema type not understood. "
            + "Use Avro, Json or Protobuf");

    }

    return schemaList;
  }

}
