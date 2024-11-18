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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import java.util.stream.Collectors;
import java.util.stream.Stream;

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

  public static void mergeNumberTypes(List<JsonNode> primitives) {
    // Checking if anyone element is double or long type
    String type = null;
    List<String> integerTypes = Arrays.asList(DeriveAvroSchema.INT,
        DeriveAvroSchema.LONG, DeriveProtobufSchema.INT_32, DeriveProtobufSchema.INT_64);
    for (String types : Arrays.asList(DeriveAvroSchema.LONG, DeriveProtobufSchema.INT_64,
        DeriveAvroSchema.DOUBLE)) {
      if (primitives.stream().anyMatch(o -> o.get("type").asText().equals(types))) {
        type = types;
      }
    }
    if (type == null) {
      return;
    }
    // if double is present, int and long types are marked as double
    // if double is absent and long is present, int is marked as long
    for (JsonNode node : primitives) {
      if (integerTypes.contains(node.get("type").asText())) {
        ((ObjectNode) node).put("type", type);
      }
    }
  }

  public static void replaceEachField(JsonNode mergedArray, List<JsonNode> uniqueRecords) {
    // Marks type, properties and name for each record using merged array
    for (JsonNode record : uniqueRecords) {
      ObjectNode objectNode = (ObjectNode) record;
      for (String field : DeriveSchemaUtils.getSortedKeys(mergedArray)) {
        objectNode.set(field, mergedArray.get(field));
      }
    }
  }

  public static ArrayNode sortJsonArrayList(ArrayNode array) {
    List<JsonNode> dataNodes = DeriveSchemaUtils.getListFromArray(array);
    Stream<JsonNode> stream = dataNodes.stream().distinct();
    Stream<JsonNode> sortedDataNodes;
    if (array.size() > 0 && dataNodes.get(0).has("type")) {
      sortedDataNodes = stream.sorted(Comparator.comparing(o -> o.get("type").asText()));
    } else {
      sortedDataNodes = stream.sorted(Comparator.comparing(JsonNode::asInt));
    }
    return JacksonMapper.INSTANCE.createArrayNode()
        .addAll(sortedDataNodes.collect(Collectors.toList()));
  }
}
