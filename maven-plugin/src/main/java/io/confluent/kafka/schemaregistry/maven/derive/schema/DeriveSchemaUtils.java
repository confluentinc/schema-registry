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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import java.util.stream.Collectors;

public class DeriveSchemaUtils {

  public static List<JsonNode> getUnique(List<JsonNode> schemas) {
    return schemas.stream().distinct().collect(Collectors.toList());
  }

  public static List<JsonNode> getListFromArray(JsonNode field) {
    List<JsonNode> objectList = new ArrayList<>();
    field.forEach(objectList::add);
    return objectList;
  }

  public static List<String> getSortedKeys(JsonNode message) {
    List<String> keys = new ArrayList<>();
    for (Iterator<String> it = message.fieldNames(); it.hasNext(); ) {
      keys.add(it.next());
    }
    Collections.sort(keys);
    return keys;
  }

  public static void groupItems(JsonNode element,
                                List<JsonNode> items,
                                List<JsonNode> records,
                                List<JsonNode> arrays) {
    if (element.isEmpty() || items.contains(element)) {
      return;
    }
    // If element is oneOf type, add all elements inside oneOf
    if (element.has("oneOf")) {
      element.get("oneOf").forEach(o -> groupItems(o, items, records, arrays));
      return;
    }

    String typeOfElement = element.get("type").asText();
    if (typeOfElement.equals("object")) {
      records.add(element);
    } else if (typeOfElement.equals("array")) {
      arrays.add(element);
    } else {
      items.add(element);
    }
  }

  public static ObjectNode sortObjectNode(ObjectNode node) {
    ObjectNode sortedObjectNode = JacksonMapper.INSTANCE.createObjectNode();
    DeriveSchemaUtils.getSortedKeys(node).forEach(key -> sortedObjectNode.set(key, node.get(key)));
    return sortedObjectNode;
  }

  public static void mergeNumberTypes(List<ObjectNode> primitives) {
    // TODO: Change constants when protobuf and avro classes are introduced
    // Checking if anyone element is double type
    if (primitives.stream().noneMatch(o -> o.get("type").asText().equals("double"))) {
      return;
    }
    // If any other element is of type integer/long, it is marked as double
    List<String> integerTypes = Arrays.asList("int", "int32", "int64", "long");
    primitives.stream().filter(o -> integerTypes.contains(o.get("type").asText()))
        .forEach(o -> o.put("type", "double"));
  }
}
