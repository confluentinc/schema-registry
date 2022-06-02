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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Optional;
import java.util.List;
import java.util.Collections;
import java.util.Iterator;


/**
 * Abstract class to provide structure for each schema type and store common functions.
 */
public abstract class DeriveSchema {

  public static final ObjectMapper mapper = new ObjectMapper();

  abstract Optional<ObjectNode> getPrimitiveSchema(Object field) throws JsonProcessingException;

  abstract ArrayList<ObjectNode> getSchemaOfAllElements(List<Object> list, String name)
      throws JsonProcessingException;

  abstract ObjectNode getSchemaForArray(List<Object> list, String name)
      throws JsonProcessingException;

  abstract ObjectNode getSchemaForRecord(ObjectNode objectNode, String name)
      throws JsonProcessingException;

  static List<Object> getListFromArray(Object field) {

    if (field instanceof ArrayNode) {
      List<Object> objectList = new ArrayList<>();
      for (int i = 0; i < ((ArrayNode) field).size(); i++) {
        objectList.add(((ArrayNode) field).get(i));
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
}
