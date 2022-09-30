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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import java.util.stream.Collectors;

public class DeriveSchemaUtils {

  public static List<ObjectNode> getUnique(List<ObjectNode> schemas) {
    return schemas
        .stream()
        .distinct()
        .collect(Collectors.toList());
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

  static void groupItems(ObjectNode element,
                         ArrayList<ObjectNode> items,
                         ArrayList<ObjectNode> records,
                         ArrayList<ObjectNode> arrays) {
    if (element.isEmpty() || items.contains(element)) {
      return;
    }
    // If element is oneOf type, add all elements inside oneOf
    if (element.has("oneOf")) {
      ArrayNode elements = (ArrayNode) element.get("oneOf");
      for (JsonNode oneOfElement : elements) {
        groupItems((ObjectNode) oneOfElement, items, records, arrays);
      }
      return;
    }

    if (element.has("type")) {
      String typeOfElement = element.get("type").asText();
      if (typeOfElement.equals("object")) {
        records.add(element);
      } else if (typeOfElement.equals("array")) {
        arrays.add(element);
      } else {
        items.add(element);
      }
    } else {
      items.add(element);
    }
  }

  static ObjectNode sortObjectNode(ObjectNode node) {
    ObjectNode sortedObjectNode = JacksonMapper.INSTANCE.createObjectNode();
    for (String key : DeriveSchemaUtils.getSortedKeys(node)) {
      sortedObjectNode.set(key, node.get(key));
    }
    return sortedObjectNode;
  }

  static void mergeNumberTypes(List<ObjectNode> primitives) {
    // Checking if anyone element is double type
    Optional<ObjectNode> decimalType = primitives.stream()
        .filter(o -> o.get("type").asText().equals("double")).findAny();
    if (!decimalType.isPresent()) {
      return;
    }

    // If any other element is of type integer/long is marked as double
    List<String> integerTypes = Arrays.asList("int", "int32", "int64", "long");
    primitives.stream()
        .filter(o -> integerTypes.contains(o.get("type").asText()))
        .forEach(o -> o.put("type", "double"));
  }
}
