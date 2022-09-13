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
import io.confluent.kafka.schemaregistry.maven.derive.schema.json.DeriveJsonSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Comparator;
import java.util.stream.Collectors;

public abstract class DeriveSchema {

  public static final ObjectMapper mapper = new ObjectMapper();

  public static ArrayNode sortJsonArrayList(ArrayNode node) {
    List<JsonNode> dataNodes = getListFromArray(node);
    // Sort items of arrayNode using type as the comparator
    List<JsonNode> sortedDataNodes = dataNodes
        .stream()
        .distinct()
        .sorted(Comparator.comparing(o -> o.get("type").asText()))
        .collect(Collectors.toList());
    //return the same Json structure as in method parameter
    return mapper.createObjectNode().arrayNode().addAll(sortedDataNodes);
  }

  public static ObjectNode sortObjectNode(ObjectNode node) {
    ObjectNode sortedObjectNode = mapper.createObjectNode();
    List<String> sortedKeys = getSortedKeys(node);
    for (String key : sortedKeys) {
      sortedObjectNode.set(key, node.get(key));
    }
    return sortedObjectNode;
  }

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

  public static List<JsonNode> getListFromArray(ArrayNode field) {
    List<JsonNode> objectList = new ArrayList<>();
    for (JsonNode fieldItem : field) {
      objectList.add(fieldItem);
    }
    return objectList;
  }

  public static List<String> getSortedKeys(ObjectNode message) {
    ArrayList<String> keys = new ArrayList<>();
    for (Iterator<String> it = message.fieldNames(); it.hasNext(); ) {
      keys.add(it.next());
    }
    Collections.sort(keys);
    return keys;
  }

  public static List<ObjectNode> getSchemaInformation(String schemaType,
                                                      boolean strictCheck,
                                                      ArrayList<String> messages)
      throws JsonProcessingException {
    List<ObjectNode> schemaList = new ArrayList<>();
    if (schemaType == null) {
      throw new IllegalArgumentException("Schema Type not set");
    }

    if ("json".equalsIgnoreCase(schemaType)) {
      ObjectNode schema = DeriveJsonSchema.getSchemaForMultipleMessages(messages);
      schemaList.add(schema);
    } else {
      throw new IllegalArgumentException("Schema type not understood. "
          + "Use Avro, Json or Protobuf");
    }
    return schemaList;
  }
}
