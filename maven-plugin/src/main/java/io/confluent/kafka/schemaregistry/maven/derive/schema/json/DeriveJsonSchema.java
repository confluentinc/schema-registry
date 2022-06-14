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

package io.confluent.kafka.schemaregistry.maven.derive.schema.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema.mapper;

public class DeriveJsonSchema {

  public DeriveJsonSchema() {}

  /**
   * Get schema for multiple messages.
   * Treated same as array of records and exactly one schema is returned
   *
   * @param messages list of messages, each message is a string
   * @return map with schema
   * @throws JsonProcessingException thrown if message not in JSON format
   */
  public ObjectNode getSchemaForMultipleMessages(List<String> messages)
      throws JsonProcessingException {

    List<Object> messageObjects = new ArrayList<>();
    for (String message : messages) {
      messageObjects.add(mapper.readTree(message));
    }

    ObjectNode schema = (ObjectNode) DeriveJsonSchemaArray.getSchemaForArray(
        messageObjects, "").get("items");
    ObjectNode schemaInformation = mapper.createObjectNode();
    schemaInformation.set("schema", schema);
    return schemaInformation;
  }

}
