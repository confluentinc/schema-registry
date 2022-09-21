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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;
import java.util.HashMap;
import java.util.Optional;
import java.util.ArrayList;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchemaUtils.getListFromArray;
import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchemaUtils.mapper;

public abstract class DeriveSchema {

  protected final HashMap<String, String> classToDataType = new HashMap<>();

  public Optional<ObjectNode> getPrimitiveSchema(Object field)
      throws JsonProcessingException {
    // Generate Schema for Primitive type
    String jsonInferredType = field.getClass().getName();
    if (classToDataType.containsKey(jsonInferredType)) {
      String schemaString = String.format("{\"type\" : \"%s\"}",
          classToDataType.get(jsonInferredType));
      return Optional.of(mapper.readValue(schemaString, ObjectNode.class));
    }
    return Optional.empty();
  }

  protected ArrayList<ObjectNode> getSchemaOfAllElements(List<JsonNode> messages, String name)
      throws JsonProcessingException {
    // Get schema of each message based on type
    ArrayList<ObjectNode> schemaList = new ArrayList<>();
    for (JsonNode message : messages) {
      schemaList.add(getSchemaOfElement(message, name));
    }
    return schemaList;
  }

  protected ObjectNode getSchemaOfElement(JsonNode message, String name)
      throws JsonProcessingException {
    // Get schema of message based on type
    Optional<ObjectNode> primitiveSchema = getPrimitiveSchema(message);
    if (primitiveSchema.isPresent()) {
      return primitiveSchema.get();
    } else if (message instanceof ArrayNode) {
      return getSchemaForArray(getListFromArray((ArrayNode) message), name);
    } else {
      ObjectNode objectNode = mapper.valueToTree(message);
      return getSchemaForRecord(objectNode);
    }
  }

  protected abstract ObjectNode getSchemaForArray(List<JsonNode> messages, String name)
      throws JsonProcessingException;

  protected abstract ObjectNode getSchemaForRecord(ObjectNode message)
      throws JsonProcessingException;
}
